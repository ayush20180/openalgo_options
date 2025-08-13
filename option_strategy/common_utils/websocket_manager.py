import asyncio
import json
import websockets
import time
import threading
from collections import deque

class WebSocketManager:
    """
    Manages a persistent WebSocket connection for real-time market data,
    with automatic reconnection and fallback to REST API polling.
    """
    def __init__(self, strategy_name: str, api_key: str, host_url: str, logger, api_caller, config: dict):
        self.strategy_name = strategy_name
        self.api_key = api_key
        self.host_url = host_url
        self.logger = logger
        self.api_caller = api_caller # A function to make REST API calls, e.g., strategy._make_api_request
        self.config = config.get('websocket', {})

        self.ws_url = self._get_ws_url()
        self.websocket = None
        self.is_connected = False
        self.subscriptions = set()
        self.lock = threading.Lock()

        self.on_tick_callback = None
        self.main_loop_task = None

    def _get_ws_url(self) -> str:
        """Constructs the WebSocket URL from the host URL."""
        if self.host_url.startswith("https://"):
            domain = self.host_url.replace("https://", "")
            return f"wss://{domain}/ws"
        else:
            domain = self.host_url.replace("http://", "")
            # Assuming ws runs on port 8765 for local http setups
            host, _, _ = domain.partition(':')
            return f"ws://{host}:8765"

    async def _run(self):
        """The main coroutine that manages the connection and message loop."""
        retries = self.config.get('websocket_max_retries', 5)

        while retries > 0:
            try:
                async with websockets.connect(self.ws_url) as ws:
                    self.websocket = ws
                    self.is_connected = True
                    self.logger.info("WebSocket connected.", extra={'event': 'WEBSOCKET'})

                    # Authenticate
                    await ws.send(json.dumps({"action": "authenticate", "api_key": self.api_key}))
                    auth_response = await ws.recv()
                    # Simple check for auth success, real app might need more robust validation
                    if "success" not in auth_response.lower():
                        self.logger.error(f"WebSocket authentication failed: {auth_response}", extra={'event': 'ERROR'})
                        break

                    # Re-subscribe to all tracked symbols
                    await self._resubscribe_all()

                    # Reset retries on successful connection
                    retries = self.config.get('websocket_max_retries', 5)

                    async for message in ws:
                        data = json.loads(message)
                        if data.get('type') == 'market_data' and self.on_tick_callback:
                            self.on_tick_callback(data['data'])

            except (websockets.exceptions.ConnectionClosedError, ConnectionRefusedError) as e:
                self.is_connected = False
                self.logger.warning(f"WebSocket disconnected: {e}. Retrying...", extra={'event': 'WEBSOCKET'})
                retries -= 1
                await asyncio.sleep(5) # Wait before retrying

            except Exception as e:
                self.is_connected = False
                self.logger.error(f"An unexpected WebSocket error occurred: {e}", extra={'event': 'ERROR'})
                retries -= 1
                await asyncio.sleep(10)

        # If all retries fail, switch to fallback
        self.logger.error("WebSocket connection failed after all retries. Switching to API polling fallback.", extra={'event': 'ERROR'})
        await self._fallback_poll()

    async def _fallback_poll(self):
        """Polls the REST API for quotes when WebSocket is down."""
        interval = self.config.get('poll_interval_fallback', 1)
        while True:
            with self.lock:
                symbols_to_poll = list(self.subscriptions)

            for topic in symbols_to_poll:
                symbol, exchange = topic.split('.')
                quote = self.api_caller('POST', 'quotes', {"symbol": symbol, "exchange": exchange})
                if quote and quote.get('status') == 'success' and self.on_tick_callback:
                    # Adapt REST response to look like a WS tick for the callback
                    tick_data = {'symbol': symbol, 'exchange': exchange, 'ltp': quote['data']['ltp']}
                    self.on_tick_callback(tick_data)

            await asyncio.sleep(interval)
            # TODO: Add logic to periodically try to reconnect to WebSocket from here

    def start_stream(self, initial_symbols: list, on_tick_callback):
        """Starts the WebSocket manager in a new thread."""
        self.on_tick_callback = on_tick_callback
        with self.lock:
            self.subscriptions.update(initial_symbols)

        def run_loop():
            asyncio.run(self._run())

        self.main_loop_task = threading.Thread(target=run_loop, daemon=True)
        self.main_loop_task.start()
        self.logger.info("WebSocket manager started.", extra={'event': 'INFO'})

    def subscribe(self, symbols: list):
        """Subscribes to a list of new symbols."""
        with self.lock:
            new_symbols = [s for s in symbols if s not in self.subscriptions]
            self.subscriptions.update(new_symbols)

        async def do_subscribe():
            if self.is_connected and self.websocket:
                for topic in new_symbols:
                    symbol, exchange = topic.split('.')
                    await self.websocket.send(json.dumps({"action": "subscribe", "symbol": symbol, "exchange": exchange, "mode": 1}))

        if self.is_connected:
            asyncio.run(do_subscribe())

    def unsubscribe(self, symbols: list):
        """Unsubscribes from a list of symbols."""
        with self.lock:
            symbols_to_remove = [s for s in symbols if s in self.subscriptions]
            for s in symbols_to_remove:
                self.subscriptions.remove(s)

        async def do_unsubscribe():
            if self.is_connected and self.websocket:
                for topic in symbols_to_remove:
                    symbol, exchange = topic.split('.')
                    await self.websocket.send(json.dumps({"action": "unsubscribe", "symbol": symbol, "exchange": exchange, "mode": 1}))

        if self.is_connected:
            asyncio.run(do_unsubscribe())

    async def _resubscribe_all(self):
        """Subscribes to all currently tracked symbols."""
        with self.lock:
            symbols_to_subscribe = list(self.subscriptions)

        if self.is_connected and self.websocket:
            for topic in symbols_to_subscribe:
                symbol, exchange = topic.split('.')
                await self.websocket.send(json.dumps({"action": "subscribe", "symbol": symbol, "exchange": exchange, "mode": 1}))
        self.logger.info(f"Resubscribed to {len(symbols_to_subscribe)} symbols.", extra={'event': 'WEBSOCKET'})
