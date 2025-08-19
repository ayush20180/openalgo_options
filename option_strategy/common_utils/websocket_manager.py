import queue
import threading
import time
from openalgo import api

class WebSocketManager:
    def __init__(self, api_key, host, ws_url, on_tick_callback, logger):
        self.api_key = api_key
        self.host = host
        self.ws_url = ws_url
        self.on_tick_callback = on_tick_callback
        self.logger = logger

        self.command_queue = queue.Queue()
        self.client = None
        self.worker_thread = threading.Thread(target=self._run, daemon=True)
        self.subscriptions = []

    def _run(self):
        self.logger.info("WebSocketManager thread started.", extra={'event': 'WS_MANAGER'})
        while True:
            try:
                command = self.command_queue.get()
                action = command.get('action')
                self.logger.info(f"WS_MANAGER: Received command: {action}", extra={'event': 'WS_MANAGER'})

                if action == 'CONNECT':
                    self._connect()
                    self._subscribe(command.get('subscriptions', []))
                elif action == 'DISCONNECT':
                    self._disconnect()
                elif action == 'RECONNECT':
                    self._disconnect()
                    self._connect()
                    self._subscribe(command.get('subscriptions', []))
                elif action == 'SHUTDOWN':
                    self._disconnect()
                    break

            except Exception as e:
                self.logger.error(f"Error in WebSocketManager thread: {e}", exc_info=True, extra={'event': 'ERROR'})
        self.logger.info("WebSocketManager thread finished.", extra={'event': 'WS_MANAGER'})

    def _connect(self):
        try:
            if self.client and self.client.is_connected():
                self.logger.warning("WS_MANAGER: Already connected.", extra={'event': 'WS_MANAGER'})
                return

            self.logger.info("WS_MANAGER: Connecting...", extra={'event': 'WS_MANAGER'})
            self.client = api(api_key=self.api_key, host=self.host, ws_url=self.ws_url)
            self.client.connect()
            # The on_tick callback is now passed during subscription
            self.logger.info("WS_MANAGER: Connected successfully.", extra={'event': 'WS_MANAGER'})
        except Exception as e:
            self.logger.error(f"WS_MANAGER: Connection failed: {e}", exc_info=True, extra={'event': 'ERROR'})

    def _disconnect(self):
        try:
            if self.client:
                self.logger.info("WS_MANAGER: Unsubscribing from all symbols.", extra={'event': 'WS_MANAGER'})
                if self.subscriptions:
                    self.client.unsubscribe_ltp(self.subscriptions)
                self.logger.info("WS_MANAGER: Disconnecting...", extra={'event': 'WS_MANAGER'})
                self.client.disconnect()
                self.logger.info("WS_MANAGER: Disconnected successfully.", extra={'event': 'WS_MANAGER'})
            self.client = None
            self.subscriptions = []
        except Exception as e:
            self.logger.error(f"WS_MANAGER: Error during disconnect: {e}", exc_info=True, extra={'event': 'ERROR'})

    def _subscribe(self, subscriptions):
        if not self.client or not subscriptions:
            return
        try:
            self.logger.info(f"WS_MANAGER: Subscribing to {subscriptions}", extra={'event': 'WS_MANAGER'})
            self.client.subscribe_ltp(subscriptions, on_data_received=self.on_tick_callback)
            self.subscriptions = subscriptions
            self.logger.info(f"WS_MANAGER: Subscribed successfully.", extra={'event': 'WS_MANAGER'})
        except Exception as e:
            self.logger.error(f"WS_MANAGER: Subscription failed: {e}", exc_info=True, extra={'event': 'ERROR'})

    def start(self):
        self.worker_thread.start()

    def stop(self):
        self.command_queue.put({'action': 'SHUTDOWN'})
        self.worker_thread.join(timeout=5)

    def connect(self, subscriptions):
        self.command_queue.put({'action': 'CONNECT', 'subscriptions': subscriptions})

    def disconnect(self):
        self.command_queue.put({'action': 'DISCONNECT'})

    def reconnect(self, subscriptions):
        self.command_queue.put({'action': 'RECONNECT', 'subscriptions': subscriptions})
