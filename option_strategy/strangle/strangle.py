import os
import json
import time
import re
import asyncio
import threading
import queue
from datetime import datetime, time as time_obj
import pytz
from dotenv import load_dotenv
import requests
import pandas as pd

from openalgo import api

from ..common_utils.logger import setup_logger
from ..common_utils.state_manager import StateManager
from ..common_utils.trade_journal import TradeJournal

class StrangleStrategy:
    def __init__(self, strategy_name: str):
        self.strategy_name = strategy_name
        self.base_path = os.path.join('option_strategy', self.strategy_name)

        self.config_path = os.path.join(self.base_path, 'config', f"{self.strategy_name}_config.json")
        self.log_path = os.path.join(self.base_path, 'logs')
        self.state_path = os.path.join(self.base_path, 'state')
        self.trades_path = os.path.join(self.base_path, 'trades')

        self._load_config()
        self.mode = self.config.get('mode', 'PAPER')

        self.logger = setup_logger(self.strategy_name, self.log_path, self.mode)
        self.state_manager = StateManager(self.state_path)
        self.journal = TradeJournal(self.strategy_name, self.trades_path, self.mode)
        self.state = self.state_manager.load_state(self.strategy_name, self.mode)

        self._setup_api_client()

        self.live_prices = {}
        self.adjustment_completion_queue = queue.Queue()
        self._sym_rx = re.compile(r"^[A-Z]+(\d{2}[A-Z]{3}\d{2})(\d+)(CE|PE)$")
        self.logger.info("Strategy initialized", extra={'event': 'INFO'})

    def _load_config(self):
        with open(self.config_path, 'r') as f:
            self.config = json.load(f)

    def _get_ws_url(self):
        host_url = os.getenv("HOST_SERVER", "")
        if host_url.startswith("https://"):
            return f"wss://{host_url.replace('https://', '')}/ws"
        elif host_url.startswith("http://"):
            return f"ws://{host_url.replace('http://', '').split(':')[0]}:8765"
        return "ws://127.0.0.1:8765"

    def _setup_api_client(self):
        dotenv_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
        load_dotenv(dotenv_path=dotenv_path, override=True)
        self.api_key = os.getenv("APP_KEY")
        self.host_server = os.getenv("HOST_SERVER")
        if not self.api_key or not self.host_server:
            raise ValueError("API credentials not found in .env file")

        self.client = api(api_key=self.api_key, host=self.host_server, ws_url=self._get_ws_url())

    def run(self):
        self.logger.info("Run - Checkpoint 1: Starting Strategy", extra={'event': 'DEBUG'})
        if not self.state.get('active_trade_id'):
            self.state = {'active_trade_id': None, 'active_legs': {}, 'adjustment_count': 0, 'is_adjusting': False, 'mode': self.mode}
        else:
            # If strategy was stopped mid-adjustment, reset the flag
            if self.state.get('is_adjusting'):
                self.logger.warning("Resetting 'is_adjusting' flag to False on startup.")
                self.state['is_adjusting'] = False
                self.state_manager.save_state(self.strategy_name, self.mode, self.state)

            # If there are active legs from a previous session, start monitoring them
            if self.state.get('active_legs'):
                self.logger.info("Active legs found on startup. Resuming monitoring.")
                self._start_monitoring()

        self.logger.info(f"Run - Checkpoint 2: Initial state: {self.state}", extra={'event': 'DEBUG'})

        start_time = time_obj.fromisoformat(self.config['start_time'])
        self.logger.info("Run - Checkpoint 3: Parsed start_time", extra={'event': 'DEBUG'})
        end_time = time_obj.fromisoformat(self.config['end_time'])
        self.logger.info("Run - Checkpoint 4: Parsed end_time", extra={'event': 'DEBUG'})
        ist = pytz.timezone("Asia/Kolkata")
        self.logger.info("Run - Checkpoint 5: Timezone set", extra={'event': 'DEBUG'})

        while True:
            try:
                # Check for completed adjustments that need subscription changes
                adj_info = self.adjustment_completion_queue.get_nowait()
                self.logger.info(f"DIAGNOSTIC: Dequeued adjustment info: {adj_info}. Processing subscription change.", extra={'event': 'QUEUE'})
                unsubscribe_list = [{"exchange": self.config['exchange'], "symbol": adj_info['old_symbol']}]
                subscribe_list = [{"exchange": self.config['exchange'], "symbol": adj_info['new_symbol']}]
                self._manage_subscriptions(unsubscribe_list=unsubscribe_list, subscribe_list=subscribe_list)
            except queue.Empty:
                pass # No adjustments to process

            self.logger.info("Run - Checkpoint 6: Top of main loop", extra={'event': 'DEBUG'})
            now_ist = datetime.now(ist).time()

            if now_ist < start_time:
                wait_seconds = (datetime.combine(datetime.now(ist).date(), start_time) - datetime.now(ist)).total_seconds()
                self.logger.info(f"Waiting for trading window to start at {start_time.strftime('%H:%M:%S')}. Sleeping for {wait_seconds:.2f} seconds.", extra={'event': 'INFO'})
                if wait_seconds > 0:
                    time.sleep(wait_seconds)
                continue

            if start_time <= now_ist < end_time:
                self.logger.info("Run - Checkpoint 8: Inside trading window", extra={'event': 'DEBUG'})
                if not self.state.get('active_trade_id'):
                    self.execute_entry()
                    if self.state.get('active_legs'):
                        self._start_monitoring()

                self.logger.info("Run - Checkpoint 9: Main thread sleeping", extra={'event': 'DEBUG'})
                time.sleep(1)
                continue

            if now_ist >= end_time:
                self.logger.info("Run - Checkpoint 10: After trading window", extra={'event': 'DEBUG'})
                self.execute_exit()
                break

    def _start_monitoring(self):
        self.logger.info("Attempting to start monitoring...", extra={'event': 'DEBUG'})
        try:
            self.client.connect()
            self.logger.info("WebSocket connected successfully.", extra={'event': 'WEBSOCKET'})
            self._manage_subscriptions()
        except Exception as e:
            self.logger.error(f"WebSocket connection failed: {e}. Switching to fallback.", extra={'event': 'ERROR'})
            self._start_fallback_poll()

    def _start_fallback_poll(self):
        self.logger.warning("Starting REST API polling fallback.", extra={'event': 'WEBSOCKET'})
        poll_thread = threading.Thread(target=self._fallback_poll_loop, daemon=True)
        poll_thread.start()

    def _fallback_poll_loop(self):
        interval = self.config['websocket'].get('poll_interval_fallback', 1)
        while True:
            active_legs_copy = list(self.state.get('active_legs', {}).values())
            for leg_info in active_legs_copy:
                symbol = leg_info['symbol']
                quote = self._make_api_request('POST', 'quotes', {"symbol": symbol, "exchange": self.config['exchange']})
                if quote and quote.get('status') == 'success':
                    self._on_tick({'symbol': symbol, 'ltp': quote['data']['ltp']})
            time.sleep(interval)

    def _manage_subscriptions(self, unsubscribe_list=None, subscribe_list=None):
        self.logger.info(f"Managing subscriptions. Unsub: {unsubscribe_list}, Sub: {subscribe_list}", extra={'event': 'DEBUG'})
        if unsubscribe_list:
            self.client.unsubscribe_ltp(unsubscribe_list)
            self.logger.info(f"Unsubscribed from: {unsubscribe_list}", extra={'event': 'WEBSOCKET'})

        if subscribe_list:
            self.logger.info(f"DIAGNOSTIC: Subscribing to {subscribe_list} with on_tick callback.", extra={'event': 'WEBSOCKET'})
            self.client.subscribe_ltp(subscribe_list, on_data_received=self._on_tick)
            self.logger.info(f"Subscribed to: {subscribe_list}", extra={'event': 'WEBSOCKET'})

        if not unsubscribe_list and not subscribe_list:
            symbols = [leg['symbol'] for leg in self.state['active_legs'].values()]
            symbols.append(self.config['index'])
            instrument_list = [{"exchange": "NSE_INDEX" if s == self.config['index'] else self.config['exchange'], "symbol": s} for s in symbols]
            self.client.subscribe_ltp(instrument_list, on_data_received=self._on_tick)
            self.logger.info(f"Initial subscription to: {instrument_list}", extra={'event': 'WEBSOCKET'})

    def execute_entry(self):
        self.logger.info("Attempting new trade entry.", extra={'event': 'ENTRY'})
        try:
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

            self.state['active_trade_id'] = self.journal.generate_trade_id()
            self.state['adjustment_count'] = 0

            self.logger.info(f"New trade started with ID: {self.state['active_trade_id']}", extra={'event': 'ENTRY'})

            self._place_leg_order("CALL_SHORT", ce_symbol, ce_strike, "SELL", is_adjustment=False)
            self._place_leg_order("PUT_SHORT", pe_symbol, pe_strike, "SELL", is_adjustment=False)

            self.state_manager.save_state(self.strategy_name, self.mode, self.state)
        except Exception as e:
            self.logger.error(f"Error during entry: {e}", extra={'event': 'ERROR'}, exc_info=True)

    def _on_tick(self, data):
        self.logger.info(f"Tick received: {data}", extra={'event': 'DEBUG'})
        try:
            symbol = data.get('symbol')
            ltp = data.get('data', {}).get('ltp')
            if symbol and ltp is not None:
                self.live_prices[symbol] = ltp
                if not self.state.get('is_adjusting'):
                    self.monitor_and_adjust()
        except Exception as e:
            self.logger.error(f"Error processing tick: {data} | Error: {e}", extra={'event': 'ERROR'})


    def monitor_and_adjust(self):
        self.logger.info(f"Monitor: state={self.state}, prices={self.live_prices}", extra={'event': 'DEBUG'})
        now_ist = datetime.now(pytz.timezone("Asia/Kolkata")).time()
        start_time = time_obj.fromisoformat(self.config['start_time'])
        end_time = time_obj.fromisoformat(self.config['end_time'])
        if not (start_time <= now_ist < end_time):
            self.logger.info(f"Monitor guard fail: Outside trading window.", extra={'event': 'DEBUG'})
            return

        if self.state.get('is_adjusting'):
            self.logger.info("Monitor guard fail: Already adjusting.", extra={'event': 'DEBUG'})
            return

        if not self.state.get('active_trade_id') or not self.config['adjustment']['enabled'] or len(self.state['active_legs']) != 2:
            self.logger.info(f"Monitor guard fail: Trade active? {bool(self.state.get('active_trade_id'))}, Adjust enabled? {self.config['adjustment']['enabled']}, Leg count: {len(self.state['active_legs'])}", extra={'event': 'DEBUG'})
            return

        ce_leg = self.state['active_legs'].get('CALL_SHORT')
        pe_leg = self.state['active_legs'].get('PUT_SHORT')
        if not ce_leg or not pe_leg:
            self.logger.info(f"Monitor guard fail: CE leg exists? {bool(ce_leg)}, PE leg exists? {bool(pe_leg)}", extra={'event': 'DEBUG'})
            return

        ce_price = self.live_prices.get(ce_leg['symbol'])
        pe_price = self.live_prices.get(pe_leg['symbol'])
        if ce_price is None or pe_price is None:
            self.logger.info(f"Monitor guard fail: CE price? {ce_price}, PE price? {pe_price}", extra={'event': 'DEBUG'})
            return

        self.logger.info(f"DIAGNOSTIC: Checking adjustment condition. CE Price={ce_price}, PE Price={pe_price}", extra={'event': 'DEBUG'})

        if ce_price < pe_price:
            smaller_price, larger_price, smaller_leg = ce_price, pe_price, 'CALL_SHORT'
        else:
            smaller_price, larger_price, smaller_leg = pe_price, ce_price, 'PUT_SHORT'

        threshold = self.config['adjustment']['threshold_ratio']
        condition_met = smaller_price < larger_price * threshold
        self.logger.info(f"DIAGNOSTIC: Condition: {smaller_price:.2f} < {larger_price:.2f} * {threshold:.2f} -> {condition_met}", extra={'event': 'DEBUG'})

        if condition_met:
            max_adjustments = self.config['adjustment'].get('max_adjustments', 5)
            if self.state['adjustment_count'] < max_adjustments:
                self.logger.info(f"Adjustment triggered for {smaller_leg} leg. Prices: {smaller_price:.2f} < {larger_price:.2f} * {threshold:.2f}", extra={'event': 'ADJUSTMENT'})
                self.state['is_adjusting'] = True
                self.state['adjustment_count'] += 1
                self.state_manager.save_state(self.strategy_name, self.mode, self.state)
                adj_thread = threading.Thread(target=self._perform_adjustment, args=(smaller_leg, larger_price), daemon=True)
                adj_thread.start()
            else:
                self.logger.warning(f"Max adjustments reached. Squaring off position.", extra={'event': 'EXIT'})
                self.execute_exit("Max adjustments reached")

    def _perform_adjustment(self, losing_leg_type: str, target_premium: float):
        self.logger.info(f"Performing adjustment for {losing_leg_type}", extra={'event': 'DEBUG'})
        losing_leg_info = self.state['active_legs'][losing_leg_type].copy()
        self._square_off_leg(losing_leg_type, losing_leg_info, is_adjustment=True)

        remaining_leg_type = 'PUT_SHORT' if losing_leg_type == 'CALL_SHORT' else 'CALL_SHORT'

        if remaining_leg_type not in self.state['active_legs']:
            self.logger.error(f"Remaining leg {remaining_leg_type} not found after squaring off. Exiting.", extra={'event': 'ERROR'})
            self.execute_exit("Remaining leg missing post-adjustment")
            self.state['is_adjusting'] = False
            self.state_manager.save_state(self.strategy_name, self.mode, self.state)
            return

        remaining_leg_strike = self.state['active_legs'][remaining_leg_type]['strike']

        new_leg_info = asyncio.run(self._find_new_leg(losing_leg_type, target_premium, losing_leg_info['strike']))
        if not new_leg_info:
            self.logger.error("Failed to find new leg. Exiting trade.", extra={'event': 'ERROR'})
            self.execute_exit("Failed to find adjustment leg")
            self.state['is_adjusting'] = False
            self.state_manager.save_state(self.strategy_name, self.mode, self.state)
            return

        new_strike = new_leg_info['strike']
        if (losing_leg_type == 'PUT_SHORT' and remaining_leg_strike < new_strike) or \
           (losing_leg_type == 'CALL_SHORT' and new_strike < remaining_leg_strike):
            self.logger.error("Inverted strangle condition met. Exiting trade.", extra={'event': 'ERROR'})
            self.execute_exit("Inverted strangle condition")
            return

        self._place_leg_order(losing_leg_type, new_leg_info['symbol'], new_leg_info['strike'], "SELL", is_adjustment=True)

        # Put a message on the queue for the main thread to process subscription changes
        adjustment_details = {
            'old_symbol': losing_leg_info['symbol'],
            'new_symbol': new_leg_info['symbol']
        }
        self.adjustment_completion_queue.put(adjustment_details)
        self.logger.info(f"DIAGNOSTIC: Added adjustment details to queue for main thread processing: {adjustment_details}", extra={'event': 'QUEUE'})

        self.state['is_adjusting'] = False
        self.state_manager.save_state(self.strategy_name, self.mode, self.state)

    async def _find_new_leg(self, option_type: str, target_premium: float, strike_to_exclude: int):
        ot = "CE" if "CALL" in option_type else "PE"
        self.logger.info(f"Finding new {ot} leg near premium {target_premium}", extra={'event': 'DEBUG'})
        index_symbol = self.config['index']
        quote_res = self._make_api_request('POST', 'quotes', {"symbol": index_symbol, "exchange": "NSE_INDEX"})
        spot_price = quote_res['data']['ltp']
        strike_interval = self.config['strike_interval'][index_symbol]
        atm_strike = int(round(spot_price / strike_interval) * strike_interval)

        radius = self.config['adjustment']['strike_search_radius']
        strikes_to_check = [atm_strike + i * strike_interval for i in range(-radius, radius + 1)]
        self.logger.info(f"DIAGNOSTIC: All candidate strikes: {strikes_to_check}", extra={'event': 'DEBUG'})

        # Exclude the strike from the leg that was just closed
        strikes_to_check = [s for s in strikes_to_check if s != strike_to_exclude]
        self.logger.info(f"DIAGNOSTIC: Filtered candidate strikes (excluding {strike_to_exclude}): {strikes_to_check}", extra={'event': 'DEBUG'})

        active_leg = next(iter(self.state['active_legs'].values()))
        m = self._sym_rx.match(active_leg['symbol'])
        expiry_str = m.group(1)

        symbols_to_check = [f"{index_symbol}{expiry_str}{k}{ot}" for k in strikes_to_check]
        self.logger.info(f"Checking {len(symbols_to_check)} strikes for new leg.", extra={'event': 'DEBUG'})

        tasks = [asyncio.to_thread(self._make_api_request, 'POST', 'quotes', {"symbol": s, "exchange": self.config['exchange']}) for s in symbols_to_check]
        results = await asyncio.gather(*tasks)

        successful_quotes = []
        for i, res in enumerate(results):
             if res and res.get('status') == 'success':
                symbol = symbols_to_check[i]
                data = res['data']
                data['symbol'] = symbol
                m = self._sym_rx.match(symbol)
                if m: data['strike'] = int(m.group(2))
                successful_quotes.append(data)

        self.logger.info(f"DIAGNOSTIC: Fetched quotes for new leg search: {successful_quotes}", extra={'event': 'DEBUG'})

        if not successful_quotes:
            self.logger.warning("Could not fetch any quotes for new leg.", extra={'event': 'DEBUG'})
            return None

        best_leg = min(successful_quotes, key=lambda q: abs(q['ltp'] - target_premium))
        self.logger.info(f"Found best new leg: {best_leg['symbol']} with price {best_leg['ltp']}", extra={'event': 'ADJUSTMENT'})
        return best_leg

    def execute_exit(self, reason="Scheduled Exit"):
        self.shutdown(reason=f"Exiting Trade: {reason}")
        self.logger.info(f"Closing trade {self.state.get('active_trade_id')} due to: {reason}", extra={'event': 'EXIT'})
        for leg_type, leg_info in list(self.state['active_legs'].items()):
            self._square_off_leg(leg_type, leg_info, is_adjustment=False)
        self.state = {}
        self.state_manager.save_state(self.strategy_name, self.mode, self.state)

    def shutdown(self, reason="Manual shutdown"):
        self.logger.info(f"DIAGNOSTIC: Shutdown initiated. Reason: {reason}", extra={'event': 'SHUTDOWN'})
        try:
            if self.client:
                self.logger.info("DIAGNOSTIC: Client object found.", extra={'event': 'SHUTDOWN'})
                unsubscribe_list = []
                # Check for active legs in state and build the unsubscribe list
                if self.state and self.state.get('active_legs'):
                    self.logger.info(f"DIAGNOSTIC: Found active legs to unsubscribe: {self.state.get('active_legs')}", extra={'event': 'SHUTDOWN'})
                    symbols = [leg['symbol'] for leg in self.state['active_legs'].values()]
                    instrument_list = [{"exchange": self.config['exchange'], "symbol": s} for s in symbols]
                    unsubscribe_list.extend(instrument_list)

                # Also unsubscribe from the index if it was being monitored
                if self.config.get('index'):
                    self.logger.info(f"DIAGNOSTIC: Found index to unsubscribe: {self.config.get('index')}", extra={'event': 'SHUTDOWN'})
                    unsubscribe_list.append({"exchange": "NSE_INDEX", "symbol": self.config['index']})

                if unsubscribe_list:
                    self.logger.info(f"DIAGNOSTIC: Calling _manage_subscriptions to unsubscribe from: {unsubscribe_list}", extra={'event': 'SHUTDOWN'})
                    self._manage_subscriptions(unsubscribe_list=unsubscribe_list)
                    self.logger.info("DIAGNOSTIC: Unsubscribe call completed. Waiting 1s.", extra={'event': 'SHUTDOWN'})
                    time.sleep(1) # Give it a moment to process
                else:
                    self.logger.info("DIAGNOSTIC: No symbols to unsubscribe.", extra={'event': 'SHUTDOWN'})

                self.logger.info("DIAGNOSTIC: Calling client.disconnect().", extra={'event': 'SHUTDOWN'})
                self.client.disconnect()
                self.logger.info("DIAGNOSTIC: client.disconnect() returned. Shutdown complete.", extra={'event': 'SHUTDOWN'})
            else:
                self.logger.warning("DIAGNOSTIC: Client object not found during shutdown.", extra={'event': 'SHUTDOWN'})

        except Exception as e:
            self.logger.error(f"DIAGNOSTIC: An error occurred during shutdown: {e}", exc_info=True, extra={'event': 'ERROR'})

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

    def _make_api_request(self, method: str, endpoint: str, payload: dict = None):
        url = f"{self.host_server}/api/v1/{endpoint}"
        req_payload = payload or {}
        req_payload['apikey'] = self.api_key
        try:
            if method.upper() == 'POST':
                response = requests.post(url, json=req_payload, timeout=10)
            else:
                response = requests.get(url, params=req_payload, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request to {endpoint} failed: {e}", extra={'event': 'ERROR'})
            return {"status": "error", "message": str(e)}
