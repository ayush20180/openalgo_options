import json
import time
from datetime import datetime, time as time_obj
import asyncio
import pytz

from .base_strategy import BaseStrategy

class StrangleStrategy(BaseStrategy):
    """
    Implements the Strangle options strategy as a standalone, runnable script.
    Inherits common functionalities from BaseStrategy.
    """

    def __init__(self, config_path: str, paper_trade_log_path: str):
        super().__init__(config_path, paper_trade_log_path)
        self.entry_executed = False

    def execute_entry(self):
        """
        Calculates the strangle strikes and places the entry orders.
        """
        self.logger.info("Executing Strangle entry logic...")
        try:
            index_symbol = self.config['index']
            exchange = self.config['exchange']

            # 1. Get Expiry Date
            expiry_res = self.get_expiry_dates(symbol=index_symbol, exchange=exchange)
            if expiry_res.get('status') != 'success' or not expiry_res.get('data'):
                self.logger.error("Could not fetch expiry dates. Halting entry.")
                return

            # Simplified expiry selection
            expiry_date = expiry_res['data'][0]
            if self.config['expiry_type'] == 'Monthly':
                expiry_date = expiry_res['data'][-1]

            def format_expiry(date_str):
                return datetime.strptime(date_str, '%d-%b-%y').strftime('%d%b%y').upper()

            formatted_expiry = format_expiry(expiry_date)
            self.logger.info(f"Using expiry: {formatted_expiry}")

            # 2. Get ATM Strike
            # The underlying index (e.g., NIFTY) is on the NSE exchange.
            quote_res = self.get_quote(symbol=index_symbol, exchange="NSE_INDEX")
            if quote_res.get('status') != 'success':
                self.logger.error(f"Could not fetch quote for {index_symbol}. Halting entry.")
                return

            spot_price = quote_res['data']['ltp']
            strike_interval = self.config['strike_interval'][index_symbol]
            atm_strike = int(round(spot_price / strike_interval) * strike_interval)
            self.logger.info(f"Spot price: {spot_price}, ATM strike: {atm_strike}")

            # 3. Calculate Strangle Strikes
            strike_diff = self.config['strike_difference'][index_symbol]
            ce_strike = atm_strike + strike_diff
            pe_strike = atm_strike - strike_diff

            # 4. Construct Symbols
            ce_symbol = f"{index_symbol}{formatted_expiry}{ce_strike}CE"
            pe_symbol = f"{index_symbol}{formatted_expiry}{pe_strike}PE"
            self.logger.info(f"Calculated symbols: {ce_symbol}, {pe_symbol}")

            # 5. Place Orders
            legs_to_place = {
                "CE": {"symbol": ce_symbol, "strike": ce_strike},
                "PE": {"symbol": pe_symbol, "strike": pe_strike}
            }

            for leg_type, leg_info in legs_to_place.items():
                self._place_leg_order(leg_type, leg_info['symbol'], leg_info['strike'], "SELL")

            self.logger.info("Entry logic completed.")

        except Exception as e:
            self.logger.error(f"An error occurred during entry execution: {e}", exc_info=True)

    def _place_leg_order(self, leg_type: str, symbol: str, strike: int, action: str):
        """Helper function to place an order for a single leg."""
        mode = self.config['mode']
        product = self.config['product_type']
        exchange = self.config['exchange']

        # Calculate the total quantity in shares
        lots = self.config['quantity_in_lots']
        lot_size = self.config['lot_size'][self.config['index']]
        total_quantity = lots * lot_size

        self.logger.info(f"Placing {action} {leg_type} order for {symbol} ({lots} lots / {total_quantity} shares) in {mode} mode.")

        if mode == 'LIVE':
            order_res = self.place_order(symbol, action, total_quantity, product, exchange)
            if order_res.get('status') == 'success':
                if action == "SELL":
                    self.active_legs[leg_type] = {'symbol': symbol, 'strike': strike, 'order_id': order_res['data']['order_id'], 'status': 'OPEN'}
                self.logger.info(f"Successfully placed LIVE order for {symbol}.")
            else:
                self.logger.error(f"Failed to place LIVE order for {symbol}: {order_res}")

        elif mode == 'PAPER':
            quote_res = self.get_quote(symbol=symbol, exchange=exchange)
            if quote_res.get('status') != 'success':
                self.logger.error(f"Could not fetch quote for {symbol} to log paper trade.")
                return

            price = quote_res['data']['ltp']
            self.log_paper_trade(symbol, action, total_quantity, price)
            if action == "SELL":
                self.active_legs[leg_type] = {'symbol': symbol, 'strike': strike, 'order_id': f'paper_{int(time.time())}', 'status': 'OPEN', 'entry_price': price}

    def monitor_and_adjust(self):
        """
        Monitors the active legs and performs adjustments if necessary.
        """
        if not self.config.get('adjustment', {}).get('enabled', False) or len(self.active_legs) != 2:
            return

        self.logger.info("Monitoring Strangle position...")
        try:
            ce_leg = self.active_legs.get('CE')
            pe_leg = self.active_legs.get('PE')

            ce_quote = self.get_quote(ce_leg['symbol'], self.config['exchange'])
            pe_quote = self.get_quote(pe_leg['symbol'], self.config['exchange'])

            if ce_quote.get('status') != 'success' or pe_quote.get('status') != 'success':
                self.logger.warning("Could not fetch quotes for active legs. Skipping adjustment check.")
                return

            ce_price = ce_quote['data']['ltp']
            pe_price = pe_quote['data']['ltp']
            self.logger.info(f"Monitoring prices: {ce_leg['symbol']}@{ce_price}, {pe_leg['symbol']}@{pe_price}")

            threshold = self.config['adjustment']['threshold_ratio']
            losing_leg_type, winning_leg_price = (None, None)

            if ce_price < pe_price * threshold:
                losing_leg_type, winning_leg_price = 'CE', pe_price
            elif pe_price < ce_price * threshold:
                losing_leg_type, winning_leg_price = 'PE', ce_price

            if losing_leg_type:
                self.logger.info(f"Adjustment triggered for {losing_leg_type} leg.")
                self._perform_adjustment(losing_leg_type, winning_leg_price)
        except Exception as e:
            self.logger.error(f"Error during adjustment: {e}", exc_info=True)

    def _perform_adjustment(self, losing_leg_type: str, target_premium: float):
        """Helper to execute the full adjustment flow."""
        self._square_off_leg(losing_leg_type)

        remaining_leg_type = 'PE' if losing_leg_type == 'CE' else 'CE'
        remaining_leg_strike = self.active_legs[remaining_leg_type]['strike']

        new_leg_info = asyncio.run(self._find_new_leg(losing_leg_type, target_premium))

        if not new_leg_info:
            self.logger.error("Could not find a suitable new leg. Exiting position for safety.")
            self.execute_exit("Failed to find adjustment leg")
            return

        new_strike = new_leg_info['strike']
        # Inversion Check
        if (losing_leg_type == 'PE' and remaining_leg_strike < new_strike) or \
           (losing_leg_type == 'CE' and new_strike < remaining_leg_strike):
            self.logger.error(f"Adjustment would cause inverted strangle. Squaring off all positions.")
            self.execute_exit("Inverted strangle condition")
            return

        self.logger.info(f"Found new leg: {new_leg_info['symbol']}. Placing order.")
        self._place_leg_order(losing_leg_type, new_leg_info['symbol'], new_leg_info['strike'], "SELL")

    async def _find_new_leg(self, option_type: str, target_premium: float) -> dict:
        """Uses async batching to find the best new leg."""
        index_symbol = self.config['index']
        exchange = self.config['exchange']

        quote_res = self.get_quote(symbol=index_symbol, exchange=f"{exchange}_INDEX")
        spot_price = quote_res['data']['ltp']
        strike_interval = self.config['strike_interval'][index_symbol]
        atm_strike = int(round(spot_price / strike_interval) * strike_interval)

        radius = self.config['adjustment']['strike_search_radius']
        strikes_to_check = [atm_strike + i * strike_interval for i in range(-radius, radius + 1)]

        active_leg = next(iter(self.active_legs.values()))
        m = self._sym_rx.match(active_leg['symbol'])
        expiry_str = m.group(1)

        symbols_to_check = [f"{index_symbol}{expiry_str}{k}{option_type}" for k in strikes_to_check]
        quotes = await self._fetch_quotes_batch(symbols_to_check)

        return min(quotes, key=lambda q: abs(q['ltp'] - target_premium)) if quotes else None

    async def _fetch_quotes_batch(self, symbols: list[str]) -> list[dict]:
        """Fetches quotes for a list of symbols in batches using asyncio."""
        tasks = [asyncio.to_thread(self.get_quote, s, self.config['exchange']) for s in symbols]
        results = await asyncio.gather(*tasks)

        successful_quotes = []
        for res, symbol in zip(results, symbols):
            if res and res.get('status') == 'success':
                res['data']['symbol'] = symbol
                m = self._sym_rx.match(symbol)
                if m:
                    res['data']['strike'] = int(m.group(2))
                successful_quotes.append(res['data'])
        return successful_quotes

    def _square_off_leg(self, leg_type: str):
        """Squares off a single leg of the strategy."""
        leg_info = self.active_legs.pop(leg_type, None)
        if not leg_info: return

        self.logger.info(f"Squaring off {leg_type} leg: {leg_info['symbol']}")
        self._place_leg_order(leg_type, leg_info['symbol'], leg_info['strike'], "BUY")

    def execute_exit(self, reason="Scheduled exit"):
        """
        Exits all open positions for the strategy.
        """
        self.logger.info(f"Executing Strangle exit: {reason}")
        active_leg_keys = list(self.active_legs.keys())
        for leg_type in active_leg_keys:
            self._square_off_leg(leg_type)

    def run(self):
        """
        The main execution loop for the strategy.
        """
        self.logger.info(f"Starting Strangle Strategy: {self.config.get('strategy_name')}")

        try:
            ist_timezone = pytz.timezone("Asia/Kolkata")
        except pytz.exceptions.UnknownTimeZoneError:
            self.logger.error("Could not find timezone 'Asia/Kolkata'. Please ensure pytz is installed correctly.")
            return

        start_time = time_obj.fromisoformat(self.config['start_time'])
        end_time = time_obj.fromisoformat(self.config['end_time'])

        self.logger.info(f"Strategy will run between {self.config['start_time']} and {self.config['end_time']} IST.")

        while True:
            now_ist = datetime.now(ist_timezone).time()

            if start_time <= now_ist < end_time:
                if not self.entry_executed:
                    self.execute_entry()
                    self.entry_executed = True

                self.monitor_and_adjust()

            elif now_ist >= end_time:
                if self.active_legs:
                    self.execute_exit()
                self.logger.info("Trading window has ended. Exiting strategy.")
                break
            else:
                self.logger.info(f"Current time {now_ist.strftime('%H:%M:%S')} is outside the trading window. Waiting...")

            time.sleep(self.config.get("monitoring_interval_seconds", 60))
