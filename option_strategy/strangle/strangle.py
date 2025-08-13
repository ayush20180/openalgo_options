import os
import json
import time
import asyncio
import re
from datetime import datetime, time as time_obj
import pytz
import requests
from dotenv import load_dotenv

from ..common_utils.logger import setup_logger
from ..common_utils.state_manager import StateManager
from ..common_utils.trade_journal import TradeJournal

class StrangleStrategy:
    """
    A production-ready, standalone Strangle strategy script with
    state persistence, detailed journaling, and structured logging.
    """
    def __init__(self, strategy_name: str):
        self.strategy_name = strategy_name
        self.base_path = os.path.join('option_strategy', self.strategy_name)

        # Setup paths
        self.config_path = os.path.join(self.base_path, 'config', f"{self.strategy_name}_config.json")
        self.log_path = os.path.join(self.base_path, 'logs')
        self.state_path = os.path.join(self.base_path, 'state')
        self.trades_path = os.path.join(self.base_path, 'trades')

        # Initialize utilities
        self.logger = setup_logger(self.strategy_name, self.log_path)
        self.state_manager = StateManager(self.state_path)
        self.journal = TradeJournal(self.strategy_name, self.trades_path)

        # Load config and state
        self._load_config()
        self.state = self.state_manager.load_state(self.strategy_name)

        self._setup_api_client()
        self._sym_rx = re.compile(r"^[A-Z]+(\d{2}[A-Z]{3}\d{2})(\d+)(CE|PE)$")
        self.logger.info("Strategy initialized", extra={'event': 'INFO'})

    def _load_config(self):
        try:
            with open(self.config_path, 'r') as f:
                self.config = json.load(f)
        except FileNotFoundError:
            self.logger.error(f"Config file not found at {self.config_path}", extra={'event': 'ERROR'})
            raise
        except json.JSONDecodeError:
            self.logger.error(f"Error decoding JSON from {self.config_path}", extra={'event': 'ERROR'})
            raise

    def _setup_api_client(self):
        dotenv_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
        load_dotenv(dotenv_path=dotenv_path, override=True)
        self.api_key = os.getenv("APP_KEY")
        self.host_server = os.getenv("HOST_SERVER")
        if not self.api_key or not self.host_server:
            self.logger.error("APP_KEY and HOST_SERVER must be set in .env file", extra={'event': 'ERROR'})
            raise ValueError("API credentials not found in .env file")

    def run(self):
        self.logger.info(f"Starting Strategy", extra={'event': 'INFO'})

        if self.state.get('active_trade_id'):
            self.logger.info(f"Resuming trade with ID: {self.state['active_trade_id']}", extra={'event': 'INFO'})
        else:
            self.state = {'active_trade_id': None, 'active_legs': {}, 'adjustment_count': 0}
            self.logger.info("No active trade found. Waiting for entry time.", extra={'event': 'INFO'})

        start_time = time_obj.fromisoformat(self.config['start_time'])
        end_time = time_obj.fromisoformat(self.config['end_time'])
        ist = pytz.timezone("Asia/Kolkata")

        while True:
            now_ist = datetime.now(ist).time()
            if start_time <= now_ist < end_time:
                if not self.state.get('active_trade_id'):
                    self.execute_entry()
                self.monitor_and_adjust()
            elif now_ist >= end_time:
                if self.state.get('active_trade_id'):
                    self.execute_exit()
                self.logger.info("Trading window ended. Exiting.", extra={'event': 'INFO'})
                break
            else:
                self.logger.info(f"Waiting for trading window. Current time: {now_ist.strftime('%H:%M:%S')} IST", extra={'event': 'INFO'})

            time.sleep(self.config.get("monitoring_interval_seconds", 60))

    def execute_entry(self):
        self.logger.info("Attempting new trade entry.", extra={'event': 'ENTRY'})

        try:
            # [Full entry logic from previous implementation]
            index_symbol = self.config['index']
            expiry_res = self._make_api_request('POST', 'expiry', {"symbol": index_symbol, "exchange": self.config['exchange'], "instrumenttype": 'options'})
            if expiry_res.get('status') != 'success': return
            expiry_date = expiry_res['data'][0]
            formatted_expiry = datetime.strptime(expiry_date, '%d-%b-%y').strftime('%d%b%y').upper()

            quote_res = self._make_api_request('POST', 'quotes', {"symbol": index_symbol, "exchange": "NSE_INDEX"})
            if quote_res.get('status') != 'success': return
            spot_price = quote_res['data']['ltp']
            strike_interval = self.config['strike_interval'][index_symbol]
            atm_strike = int(round(spot_price / strike_interval) * strike_interval)

            strike_diff = self.config['strike_difference'][index_symbol]
            ce_strike, pe_strike = atm_strike + strike_diff, atm_strike - strike_diff
            ce_symbol, pe_symbol = f"{index_symbol}{formatted_expiry}{ce_strike}CE", f"{index_symbol}{formatted_expiry}{pe_strike}PE"

            # Start of a new trade lifecycle
            self.state['active_trade_id'] = self.journal.generate_trade_id()
            self.state['adjustment_count'] = 0
            self.logger.info(f"New trade started with ID: {self.state['active_trade_id']}", extra={'event': 'ENTRY'})

            self._place_leg_order("CALL_SHORT", ce_symbol, ce_strike, "SELL", is_adjustment=False)
            self._place_leg_order("PUT_SHORT", pe_symbol, pe_strike, "SELL", is_adjustment=False)

            self.state_manager.save_state(self.strategy_name, self.state)
        except Exception as e:
            self.logger.error(f"Error during entry: {e}", extra={'event': 'ERROR'}, exc_info=True)

    def monitor_and_adjust(self):
        if not self.state.get('active_trade_id') or not self.config['adjustment']['enabled'] or len(self.state['active_legs']) != 2:
            return

        max_adjustments = self.config['adjustment'].get('max_adjustments', 5)
        if self.state['adjustment_count'] >= max_adjustments:
            if self.state.get('max_adjustments_logged') is None:
                self.logger.warning(f"Max adjustments ({max_adjustments}) reached. No further adjustments will be made.", extra={'event': 'ADJUSTMENT'})
                self.state['max_adjustments_logged'] = True
            return

        self.logger.info("Monitoring position.", extra={'event': 'INFO'})

        try:
            ce_leg = self.state['active_legs'].get('CALL_SHORT')
            pe_leg = self.state['active_legs'].get('PUT_SHORT')
            if not ce_leg or not pe_leg: return

            ce_quote = self._make_api_request('POST', 'quotes', {"symbol": ce_leg['symbol'], "exchange": self.config['exchange']})
            pe_quote = self._make_api_request('POST', 'quotes', {"symbol": pe_leg['symbol'], "exchange": self.config['exchange']})
            if ce_quote.get('status') != 'success' or pe_quote.get('status') != 'success': return

            ce_price, pe_price = ce_quote['data']['ltp'], pe_quote['data']['ltp']
            self.logger.info(f"Monitoring prices: {ce_leg['symbol']}@{ce_price}, {pe_leg['symbol']}@{pe_price}", extra={'event': 'INFO'})

            threshold = self.config['adjustment']['threshold_ratio']
            losing_leg_type, winning_leg_price = (None, None)

            if ce_price < pe_price * threshold:
                losing_leg_type, winning_leg_price = 'CALL_SHORT', pe_price
            elif pe_price < ce_price * threshold:
                losing_leg_type, winning_leg_price = 'PUT_SHORT', pe_price

            if losing_leg_type:
                self.logger.info(f"Adjustment triggered for {losing_leg_type} leg.", extra={'event': 'ADJUSTMENT'})
                self.state['adjustment_count'] += 1
                self._perform_adjustment(losing_leg_type, winning_leg_price)
                self.state_manager.save_state(self.strategy_name, self.state)

        except Exception as e:
            self.logger.error(f"Error during adjustment: {e}", extra={'event': 'ERROR'}, exc_info=True)

    def _perform_adjustment(self, losing_leg_type: str, target_premium: float):
        losing_leg_info = self.state['active_legs'][losing_leg_type]
        self._square_off_leg(losing_leg_type, losing_leg_info, is_adjustment=True)

        remaining_leg_type = 'PUT_SHORT' if losing_leg_type == 'CALL_SHORT' else 'CALL_SHORT'
        remaining_leg_strike = self.state['active_legs'][remaining_leg_type]['strike']

        new_leg_info = asyncio.run(self._find_new_leg(losing_leg_type, target_premium))

        if not new_leg_info:
            self.logger.error("Could not find a suitable new leg. Exiting position for safety.", extra={'event': 'EXIT'})
            self.execute_exit("Failed to find adjustment leg")
            return

        new_strike = new_leg_info['strike']
        # Inversion Check
        if (losing_leg_type == 'PUT_SHORT' and remaining_leg_strike < new_strike) or \
           (losing_leg_type == 'CALL_SHORT' and new_strike < remaining_leg_strike):
            self.logger.error(f"Adjustment would cause inverted strangle. Squaring off all positions.", extra={'event': 'EXIT'})
            self.execute_exit("Inverted strangle condition")
            return

        self.logger.info(f"Found new leg: {new_leg_info['symbol']}. Placing order.", extra={'event': 'ADJUSTMENT'})
        self._place_leg_order(losing_leg_type, new_leg_info['symbol'], new_leg_info['strike'], "SELL", is_adjustment=True)

    async def _find_new_leg(self, option_type: str, target_premium: float) -> dict:
        # option_type will be 'CALL_SHORT' or 'PUT_SHORT', need to extract CE/PE
        ot = "CE" if "CALL" in option_type else "PE"

        index_symbol = self.config['index']
        quote_res = self._make_api_request('POST', 'quotes', {"symbol": index_symbol, "exchange": "NSE_INDEX"})
        spot_price = quote_res['data']['ltp']
        strike_interval = self.config['strike_interval'][index_symbol]
        atm_strike = int(round(spot_price / strike_interval) * strike_interval)

        radius = self.config['adjustment']['strike_search_radius']
        strikes_to_check = [atm_strike + i * strike_interval for i in range(-radius, radius + 1)]

        active_leg = next(iter(self.state['active_legs'].values()))
        m = self._sym_rx.match(active_leg['symbol'])
        expiry_str = m.group(1)

        symbols_to_check = [f"{index_symbol}{expiry_str}{k}{ot}" for k in strikes_to_check]
        quotes = await self._fetch_quotes_batch(symbols_to_check)

        return min(quotes, key=lambda q: abs(q['ltp'] - target_premium)) if quotes else None

    async def _fetch_quotes_batch(self, symbols: list[str]) -> list[dict]:
        tasks = [asyncio.to_thread(self._make_api_request, 'POST', 'quotes', {"symbol": s, "exchange": self.config['exchange']}) for s in symbols]
        results = await asyncio.gather(*tasks)

        successful_quotes = []
        for res, symbol in zip(results, symbols):
            if res and res.get('status') == 'success':
                res['data']['symbol'] = symbol
                m = self._sym_rx.match(symbol)
                if m: res['data']['strike'] = int(m.group(2))
                successful_quotes.append(res['data'])
        return successful_quotes

    def execute_exit(self, reason="Scheduled Exit"):
        if not self.state.get('active_trade_id'): return
        self.logger.info(f"Closing trade {self.state['active_trade_id']} due to: {reason}", extra={'event': 'EXIT'})

        for leg_type, leg_info in list(self.state['active_legs'].items()):
            self._square_off_leg(leg_type, leg_info, is_adjustment=False)

        self.state = {'active_trade_id': None, 'active_legs': {}, 'adjustment_count': 0}
        self.state_manager.save_state(self.strategy_name, self.state)
        self.logger.info("Trade closed and state reset.", extra={'event': 'EXIT'})

    def _place_leg_order(self, leg_type: str, symbol: str, strike: int, action: str, is_adjustment: bool):
        mode = self.config['mode']
        lots = self.config['quantity_in_lots']
        lot_size = self.config['lot_size'][self.config['index']]
        total_quantity = lots * lot_size

        self.logger.info(f"Placing {action} {leg_type} order for {symbol}", extra={'event': 'ORDER'})

        if mode == 'LIVE':
            payload = {"symbol": symbol, "action": action, "quantity": str(total_quantity), "product": self.config['product_type'], "exchange": self.config['exchange'], "pricetype": "MARKET", "strategy": self.strategy_name}
            order_res = self._make_api_request('POST', 'placeorder', payload)
            if order_res.get('status') == 'success':
                order_id = order_res.get('orderid')
                # For live, we can't get execution price immediately from this call, assuming 0 for now.
                self.journal.record_trade(self.state['active_trade_id'], order_id, action, symbol, total_quantity, 0, leg_type, is_adjustment, mode)
                if action == "SELL": self.state['active_legs'][leg_type] = {'symbol': symbol, 'strike': strike}
            else:
                self.logger.error(f"Failed to place LIVE order for {symbol}", extra={'event': 'ERROR'})
        elif mode == 'PAPER':
            quote_res = self._make_api_request('POST', 'quotes', {"symbol": symbol, "exchange": self.config['exchange']})
            if quote_res.get('status') == 'success':
                price = quote_res['data']['ltp']
                order_id = f'paper_{int(time.time())}'
                self.journal.record_trade(self.state['active_trade_id'], order_id, action, symbol, total_quantity, price, leg_type, is_adjustment, mode)
                if action == "SELL": self.state['active_legs'][leg_type] = {'symbol': symbol, 'strike': strike}
            else:
                self.logger.error(f"Could not fetch quote for {symbol} for paper trade.", extra={'event': 'ERROR'})

    def _square_off_leg(self, leg_type: str, leg_info: dict, is_adjustment: bool):
        self.logger.info(f"Squaring off {leg_type} leg: {leg_info['symbol']}", extra={'event': 'ADJUSTMENT' if is_adjustment else 'EXIT'})
        self._place_leg_order(leg_type, leg_info['symbol'], leg_info['strike'], "BUY", is_adjustment=is_adjustment)
        self.state['active_legs'].pop(leg_type, None)

    # --- API Helper methods ---
    def _make_api_request(self, method: str, endpoint: str, payload: dict = None):
        url = f"{self.host_server}/api/v1/{endpoint}"
        req_payload = payload or {}
        req_payload['apikey'] = self.api_key
        try:
            response = requests.post(url, json=req_payload, timeout=10) if method.upper() == 'POST' else requests.get(url, params=req_payload, timeout=10)
            if response.status_code != 200:
                self.logger.error(f"API Error on {endpoint}: {response.status_code} {response.text}", extra={'event': 'ERROR'})
                return {"status": "error", "message": response.text}
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request to {endpoint} failed: {e}", extra={'event': 'ERROR'})
            return {"status": "error", "message": str(e)}

    # ... [The complex adjustment logic will be added in a subsequent step to keep this manageable] ...
