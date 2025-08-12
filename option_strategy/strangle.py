import os
import json
import logging
import asyncio
import re
import time
from datetime import datetime
import pandas as pd

from .base_strategy import BaseStrategy
from core.utils import format_expiry_date
from openalgo import api

class StrangleStrategy(BaseStrategy):
    """
    Implements the Strangle options strategy.
    """

    def __init__(self, strategy_config_path: str, client: api, logger: logging.Logger):
        """
        Initializes the StrangleStrategy.

        Args:
            strategy_config_path (str): Path to the strategy's JSON config file.
            client (api): The OpenAlgo API client instance.
            logger (logging.Logger): The logger instance for this strategy.
        """
        with open(strategy_config_path, 'r') as f:
            strategy_config = json.load(f)

        super().__init__(strategy_config, client, logger)

        self.logger.info(f"Strangle Strategy '{self.strategy_config['strategy_name']}' initialized.")
        self.entry_executed = False
        self.exit_executed = False
        self._sym_rx = re.compile(r"^[A-Z]+(\d{2}[A-Z]{3}\d{2})(\d+)(CE|PE)$")


    def run(self):
        """The main entry point for the strategy."""
        self.logger.info(f"Running strategy: {self.strategy_config['strategy_name']}")
        self.execute_entry()
        # In a real scenario, monitor_and_adjust would run in a loop.
        # This will be handled by the main scheduler loop.

    def execute_entry(self):
        """
        Calculates the strangle strikes and places the entry orders.
        """
        if self.entry_executed:
            self.logger.warning("Entry has already been executed. Skipping.")
            return

        self.logger.info("Executing entry logic...")
        try:
            # 1. Get Expiry Date
            # For "Weekly", we take the first available expiry.
            # For "Monthly", we'd need more logic to find the last Thursday.
            # Using the 'expiry' API endpoint.
            index_symbol = self.strategy_config['index']
            expiry_res = self.client.expiry(symbol=index_symbol, exchange=self.strategy_config['exchange'], instrumenttype='options')
            if expiry_res.get('status') != 'success' or not expiry_res.get('data'):
                self.logger.error("Could not fetch expiry dates.")
                return

            # Simple logic: pick the first for weekly, last for monthly.
            # This can be refined later if needed.
            if self.strategy_config['expiry_type'] == 'Weekly':
                expiry_date = expiry_res['data'][0]
            elif self.strategy_config['expiry_type'] == 'Monthly':
                # Find the last expiry date in the list which corresponds to a Thursday
                # This is a simplification; a more robust method would check calendar days.
                monthly_expiries = [e for e in expiry_res['data'] if datetime.strptime(e, '%d-%b-%y').weekday() == 3]
                expiry_date = monthly_expiries[-1] if monthly_expiries else expiry_res['data'][-1]
            else: # Daily/Intraday not supported for expiry selection
                expiry_date = expiry_res['data'][0]

            formatted_expiry = format_expiry_date(expiry_date)
            self.logger.info(f"Using expiry: {formatted_expiry}")

            # 2. Get ATM Strike
            quote_res = self.client.quotes(symbol=index_symbol, exchange=f"{self.strategy_config['exchange']}_INDEX")
            if quote_res.get('status') != 'success':
                self.logger.error(f"Could not fetch quote for {index_symbol}.")
                return

            spot_price = quote_res['data']['ltp']
            strike_interval = self.strategy_config['strike_interval'][index_symbol]
            atm_strike = int(round(spot_price / strike_interval) * strike_interval)
            self.logger.info(f"Spot price: {spot_price}, ATM strike: {atm_strike}")

            # 3. Calculate Strangle Strikes
            strike_diff = self.strategy_config['strike_difference'][index_symbol]
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
                self._place_leg_order(leg_type, leg_info['symbol'], leg_info['strike'])

            self.entry_executed = True
            self.logger.info("Entry logic completed.")

        except Exception as e:
            self.logger.error(f"An error occurred during entry execution: {e}", exc_info=True)

    def _place_leg_order(self, leg_type: str, symbol: str, strike: int):
        """Helper function to place an order for a single leg."""
        mode = self.strategy_config['mode']
        self.logger.info(f"Placing {leg_type} order for {symbol} in {mode} mode.")

        if mode == 'LIVE':
            # TODO: Implement actual order placement
            self.logger.warning("LIVE mode not fully implemented for order placement.")
            # order_res = self.client.placeorder(...)
            # For now, we simulate success
            order_res = {'status': 'success', 'data': {'order_id': f'live_{int(time.time())}'}}
            if order_res.get('status') == 'success':
                self.active_legs[leg_type] = {'symbol': symbol, 'strike': strike, 'order_id': order_res['data']['order_id'], 'status': 'OPEN'}
                self.logger.info(f"Successfully placed LIVE order for {symbol}.")
            else:
                self.logger.error(f"Failed to place LIVE order for {symbol}: {order_res}")

        elif mode == 'PAPER':
            quote_res = self.client.quotes(symbol=symbol, exchange=self.strategy_config['exchange'])
            if quote_res.get('status') != 'success':
                self.logger.error(f"Could not fetch quote for {symbol} to log paper trade.")
                return

            price = quote_res['data']['ltp']
            log_entry = {
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'strategy_name': self.strategy_config['strategy_name'],
                'mode': mode,
                'symbol': symbol,
                'action': 'SELL',
                'quantity': self.strategy_config['quantity_in_lots'],
                'price': price,
                'order_status': 'FILLED'
            }
            # Append to CSV
            log_df = pd.DataFrame([log_entry])
            log_df.to_csv('logs/paper_trades.csv', mode='a', header=not os.path.exists('logs/paper_trades.csv'), index=False)

            self.active_legs[leg_type] = {'symbol': symbol, 'strike': strike, 'order_id': f'paper_{int(time.time())}', 'status': 'OPEN', 'entry_price': price}
            self.logger.info(f"Successfully logged PAPER trade for {symbol} at price {price}.")

    def monitor_and_adjust(self):
        """
        Monitors the active legs and performs adjustments if necessary.
        This method is designed to be called periodically by the main scheduler loop.
        """
        if not self.strategy_config['adjustment']['enabled']:
            return

        if len(self.active_legs) != 2:
            self.logger.debug("Adjustment logic requires exactly 2 active legs.")
            return

        try:
            ce_leg = self.active_legs.get('CE')
            pe_leg = self.active_legs.get('PE')

            # Fetch current prices for both legs
            ce_quote_res = self.client.quotes(symbol=ce_leg['symbol'], exchange=self.strategy_config['exchange'])
            pe_quote_res = self.client.quotes(symbol=pe_leg['symbol'], exchange=self.strategy_config['exchange'])

            if ce_quote_res.get('status') != 'success' or pe_quote_res.get('status') != 'success':
                self.logger.warning("Could not fetch quotes for active legs. Skipping adjustment check.")
                return

            ce_price = ce_quote_res['data']['ltp']
            pe_price = pe_quote_res['data']['ltp']
            self.logger.info(f"Monitoring prices: {ce_leg['symbol']}@{ce_price}, {pe_leg['symbol']}@{pe_price}")

            threshold = self.strategy_config['adjustment']['threshold_ratio']
            losing_leg_type = None
            winning_leg_price = None

            if ce_price < pe_price * threshold:
                losing_leg_type = 'CE'
                winning_leg_price = pe_price
            elif pe_price < ce_price * threshold:
                losing_leg_type = 'PE'
                winning_leg_price = ce_price

            if losing_leg_type:
                self.logger.info(f"Adjustment triggered for {losing_leg_type} leg.")
                self._perform_adjustment(losing_leg_type, winning_leg_price)

        except Exception as e:
            self.logger.error(f"An error occurred during monitoring and adjustment: {e}", exc_info=True)

    def _perform_adjustment(self, losing_leg_type: str, target_premium: float):
        """Helper to execute the full adjustment flow."""

        # 1. Square off the losing leg
        self._square_off_leg(losing_leg_type)

        # 2. Find a new leg
        remaining_leg_type = 'PE' if losing_leg_type == 'CE' else 'CE'
        remaining_leg_strike = self.active_legs[remaining_leg_type]['strike']

        self.logger.info(f"Searching for new {losing_leg_type} leg with premium close to {target_premium}")

        # Use asyncio to fetch quotes in batches
        new_leg_info = asyncio.run(self._find_new_leg(losing_leg_type, target_premium))

        if not new_leg_info:
            self.logger.error("Could not find a suitable new leg. Exiting position for safety.")
            self.execute_exit("Failed to find adjustment leg")
            return

        # 3. Inversion Check
        new_strike = new_leg_info['strike']
        if losing_leg_type == 'PE': # New leg is a Put
            if remaining_leg_strike < new_strike: # CE strike < new PE strike
                self.logger.error(f"Adjustment would cause inverted strangle (CE@{remaining_leg_strike} < PE@{new_strike}). Squaring off all positions.")
                self.execute_exit("Inverted strangle condition")
                return
        else: # New leg is a Call
            if new_strike < remaining_leg_strike: # new CE strike < PE strike
                self.logger.error(f"Adjustment would cause inverted strangle (CE@{new_strike} < PE@{remaining_leg_strike}). Squaring off all positions.")
                self.execute_exit("Inverted strangle condition")
                return

        # 4. Place the new leg order
        self.logger.info(f"Found new leg: {new_leg_info['symbol']} with price {new_leg_info['ltp']}. Placing order.")
        self._place_leg_order(losing_leg_type, new_leg_info['symbol'], new_leg_info['strike'])
        self.logger.info("Adjustment completed successfully.")

    async def _find_new_leg(self, option_type: str, target_premium: float) -> dict:
        """Uses async batching to find the best new leg."""
        index_symbol = self.strategy_config['index']

        # Get current ATM to define a search radius
        quote_res = self.client.quotes(symbol=index_symbol, exchange=f"{self.strategy_config['exchange']}_INDEX")
        spot_price = quote_res['data']['ltp']
        strike_interval = self.strategy_config['strike_interval'][index_symbol]
        atm_strike = int(round(spot_price / strike_interval) * strike_interval)

        radius = self.strategy_config['adjustment']['strike_search_radius']
        strikes_to_check = [atm_strike + i * strike_interval for i in range(-radius, radius + 1)]

        # Get the same expiry as the current active leg
        active_leg = next(iter(self.active_legs.values()))
        m = self._sym_rx.match(active_leg['symbol'])
        expiry_str = m.group(1)

        symbols_to_check = [f"{index_symbol}{expiry_str}{k}{option_type}" for k in strikes_to_check]

        quotes = await self._fetch_quotes_batch(symbols_to_check)

        if not quotes:
            return None

        # Find the quote with the premium closest to the target
        best_leg = min(quotes, key=lambda q: abs(q['ltp'] - target_premium))
        return best_leg

    async def _fetch_quotes_batch(self, symbols: list[str]) -> list[dict]:
        """Fetches quotes for a list of symbols in batches using asyncio."""
        rows = []
        batch_size = 10  # As per user example
        batch_pause = 1.2 # As per user example

        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            # In a real async context, we wouldn't block with client calls.
            # The provided 'openalgo' library seems synchronous, so we use to_thread.
            res = await asyncio.gather(*[asyncio.to_thread(self._fetch_single_quote, s) for s in batch])
            rows.extend(r for r in res if r)
            if i + batch_size < len(symbols):
                await asyncio.sleep(batch_pause)
        return rows

    def _fetch_single_quote(self, symbol: str) -> dict | None:
        """Wrapper for a single quote call for use with asyncio.to_thread."""
        q = self.client.quotes(symbol=symbol, exchange=self.strategy_config['exchange'])
        if q.get("status") == "success":
            strike, opt = self._parse_symbol(symbol)
            if strike is None:
                return None
            return dict(symbol=symbol, strike=strike, type=opt, oi=q["data"]["oi"], ltp=q["data"]["ltp"])
        return None

    def _parse_symbol(self, sym: str):
        """Parses symbol to get strike and option type."""
        m = self._sym_rx.match(sym)
        return (int(m.group(2)), m.group(3)) if m else (None, None)

    def _square_off_leg(self, leg_type: str):
        """Squares off a single leg of the strategy."""
        leg_info = self.active_legs.get(leg_type)
        if not leg_info or leg_info.get('status') != 'OPEN':
            self.logger.warning(f"Attempted to square off non-active leg: {leg_type}")
            return

        symbol = leg_info['symbol']
        self.logger.info(f"Squaring off {leg_type} leg: {symbol}")

        # Using the same logic as execute_exit but for a single leg
        mode = self.strategy_config['mode']
        if mode == 'LIVE':
            # order_res = self.client.placeorder(action="BUY", ...)
            self.logger.warning(f"LIVE mode not fully implemented for squaring off {symbol}.")
            del self.active_legs[leg_type]
        elif mode == 'PAPER':
            quote_res = self.client.quotes(symbol=symbol, exchange=self.strategy_config['exchange'])
            price = quote_res['data']['ltp'] if quote_res.get('status') == 'success' else 0
            log_entry = {
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'strategy_name': self.strategy_config['strategy_name'],
                'mode': mode,
                'symbol': symbol,
                'action': 'BUY',
                'quantity': self.strategy_config['quantity_in_lots'],
                'price': price,
                'order_status': 'FILLED'
            }
            log_df = pd.DataFrame([log_entry])
            log_df.to_csv('logs/paper_trades.csv', mode='a', header=False, index=False)
            self.logger.info(f"Successfully logged paper square off for {symbol} at {price}.")
            del self.active_legs[leg_type]

    def execute_exit(self, reason: str = "Scheduled exit"):
        """
        Squares off all open positions for the strategy.
        """
        if self.exit_executed:
            self.logger.warning("Exit has already been executed. Skipping.")
            return

        self.logger.info(f"Executing exit logic. Reason: {reason}")

        active_leg_keys = list(self.active_legs.keys())
        for leg_type in active_leg_keys:
            leg_info = self.active_legs.get(leg_type)
            if not leg_info or leg_info.get('status') != 'OPEN':
                continue

            symbol = leg_info['symbol']
            mode = self.strategy_config['mode']
            self.logger.info(f"Closing {leg_type} position for {symbol} in {mode} mode.")

            if mode == 'LIVE':
                # TODO: Implement actual order placement for exit
                self.logger.warning(f"LIVE mode not fully implemented for exiting {symbol}.")
                # order_res = self.client.placeorder(action="BUY", ...)
                order_res = {'status': 'success', 'data': {'order_id': f'live_exit_{int(time.time())}'}}
                if order_res.get('status') == 'success':
                    self.logger.info(f"Successfully placed LIVE exit order for {symbol}.")
                    del self.active_legs[leg_type]
                else:
                    self.logger.error(f"Failed to place LIVE exit order for {symbol}: {order_res}")

            elif mode == 'PAPER':
                quote_res = self.client.quotes(symbol=symbol, exchange=self.strategy_config['exchange'])
                if quote_res.get('status') != 'success':
                    self.logger.error(f"Could not fetch quote for {symbol} to log paper exit.")
                    continue

                price = quote_res['data']['ltp']
                log_entry = {
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'strategy_name': self.strategy_config['strategy_name'],
                    'mode': mode,
                    'symbol': symbol,
                    'action': 'BUY', # Closing a SELL position
                    'quantity': self.strategy_config['quantity_in_lots'],
                    'price': price,
                    'order_status': 'FILLED'
                }
                log_df = pd.DataFrame([log_entry])
                log_df.to_csv('logs/paper_trades.csv', mode='a', header=False, index=False)

                self.logger.info(f"Successfully logged PAPER exit for {symbol} at price {price}.")
                del self.active_legs[leg_type]

        self.exit_executed = True
        self.logger.info("Exit logic completed.")
