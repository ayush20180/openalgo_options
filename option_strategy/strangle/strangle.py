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
        self.subscription_queue = queue.Queue()
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
        self.logger.info(f"Run - Checkpoint 2: Initial state: {self.state}", extra={'event': 'DEBUG'})

        start_time = time_obj.fromisoformat(self.config['start_time'])
        end_time = time_obj.fromisoformat(self.config['end_time'])
        ist = pytz.timezone("Asia/Kolkata")
        self.logger.info("Run - Checkpoint 3: Time objects created", extra={'event': 'DEBUG'})

        while True:
            self.logger.info("Run - Checkpoint 4: Top of main loop", extra={'event': 'DEBUG'})
            now_ist = datetime.now(ist).time()

            if now_ist < start_time:
                wait_seconds = (datetime.combine(datetime.now(ist).date(), start_time) - datetime.now(ist)).total_seconds()
                self.logger.info(f"Waiting for trading window. Sleeping for {wait_seconds:.2f} seconds.", extra={'event': 'INFO'})
                time.sleep(max(1, wait_seconds + 1)) # Add 1s buffer
                continue

            if start_time <= now_ist < end_time:
                self.logger.info("Run - Checkpoint 5: Inside trading window", extra={'event': 'DEBUG'})
                if not self.state.get('active_trade_id'):
                    self.execute_entry()
                    if self.state.get('active_legs'):
                        self._start_monitoring()

                self.logger.info("Run - Checkpoint 6: Main thread sleeping", extra={'event': 'DEBUG'})
                time.sleep(60)
                continue

            if now_ist >= end_time:
                self.logger.info("Run - Checkpoint 7: After trading window", extra={'event': 'DEBUG'})
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
            self.client.subscribe_ltp(subscribe_list)
            self.logger.info(f"Subscribed to: {subscribe_list}", extra={'event': 'WEBSOCKET'})

        if not unsubscribe_list and not subscribe_list:
            symbols = [leg['symbol'] for leg in self.state['active_legs'].values()]
            symbols.append(self.config['index'])
            instrument_list = [{"exchange": "NSE_INDEX" if s == self.config['index'] else self.config['exchange'], "symbol": s} for s in symbols]
            self.client.subscribe_ltp(instrument_list, on_data_received=self._on_tick)
            self.logger.info(f"Initial subscription to: {instrument_list}", extra={'event': 'WEBSOCKET'})

    def execute_entry(self):
        # ... (Same as before) ...
        pass

    def _on_tick(self, data):
        self.logger.info(f"Tick received: {data}", extra={'event': 'DEBUG'})
        try:
            self._process_subscription_queue()
            symbol = data.get('symbol')
            ltp = data.get('data', {}).get('ltp')
            if symbol and ltp is not None:
                self.live_prices[symbol] = ltp
                if not self.state.get('is_adjusting'):
                    self.monitor_and_adjust()
        except Exception as e:
            self.logger.error(f"Error processing tick: {data} | Error: {e}", extra={'event': 'ERROR'})

    def _process_subscription_queue(self):
        if not self.subscription_queue.empty():
            self.logger.info("Processing subscription queue.", extra={'event': 'DEBUG'})
            try:
                message = self.subscription_queue.get_nowait()
                self._manage_subscriptions(unsubscribe_list=message.get('unsubscribe'), subscribe_list=message.get('subscribe'))
            except queue.Empty:
                pass
            except Exception as e:
                self.logger.error(f"Error processing subscription queue: {e}", extra={'event': 'ERROR'})

    def monitor_and_adjust(self):
        self.logger.info(f"Monitor: state={self.state}, prices={self.live_prices}", extra={'event': 'DEBUG'})
        # ... (Same logic as before, with the "exit on max adjustments" feature) ...
        pass

    def _perform_adjustment(self, losing_leg_type: str, target_premium: float):
        self.logger.info(f"Performing adjustment for {losing_leg_type}", extra={'event': 'DEBUG'})
        # ... (Same logic as before, putting message on queue) ...
        pass

    async def _find_new_leg(self, option_type: str, target_premium: float):
        self.logger.info(f"Finding new leg for {option_type}", extra={'event': 'DEBUG'})
        # ... (Same logic as before) ...
        pass

    def execute_exit(self, reason="Scheduled Exit"):
        self.logger.info(f"Executing exit. Reason: {reason}", extra={'event': 'EXIT'})
        self.shutdown()
        for leg_type, leg_info in list(self.state['active_legs'].items()):
            self._square_off_leg(leg_type, leg_info, is_adjustment=False)
        self.state = {}
        self.state_manager.save_state(self.strategy_name, self.mode, self.state)

    def shutdown(self, reason="Manual shutdown"):
        self.logger.info(f"Shutting down WebSocket. Reason: {reason}", extra={'event': 'INFO'})
        try:
            if self.client: self.client.disconnect()
        except Exception: pass

    # ... (All other helper methods) ...
