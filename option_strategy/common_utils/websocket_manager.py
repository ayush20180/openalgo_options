import threading
from openalgo import api
import time

class WebSocketManager:
    def __init__(self, api_key, host, ws_url, on_tick_callback, logger):
        self.api_key = api_key
        self.host = host
        self.ws_url = ws_url
        self.on_tick_callback = on_tick_callback
        self.logger = logger

        self.client = None
        self.lock = threading.Lock()
        self.worker_thread = None
        self.subscriptions = []
        self.is_running = True

    def _connect_and_subscribe_thread(self, subscriptions):
        """This function runs in a dedicated thread."""
        with self.lock:
            self.logger.info("WS_MANAGER: Starting connection thread.", extra={'event': 'WS_MANAGER'})
            try:
                self.client = api(api_key=self.api_key, host=self.host, ws_url=self.ws_url)
                self.client.connect()
                self.logger.info("WS_MANAGER: Connected successfully.", extra={'event': 'WS_MANAGER'})

                if subscriptions:
                    self.logger.info(f"WS_MANAGER: Subscribing to {subscriptions}", extra={'event': 'WS_MANAGER'})
                    # This call is blocking and will take over the thread.
                    self.client.subscribe_ltp(subscriptions, on_data_received=self.on_tick_callback)
                    self.subscriptions = subscriptions

                self.logger.info("WS_MANAGER: Subscription call returned. Thread may exit.", extra={'event': 'WS_MANAGER'})

            except Exception as e:
                self.logger.error(f"WS_MANAGER: Error in connection thread: {e}", exc_info=True, extra={'event': 'ERROR'})

            self.logger.info("WS_MANAGER: Connection thread finished.", extra={'event': 'WS_MANAGER'})

    def connect(self, subscriptions):
        with self.lock:
            if self.worker_thread and self.worker_thread.is_alive():
                self.logger.warning("WS_MANAGER: connect called but a worker thread is already running.", extra={'event': 'WS_MANAGER'})
                return

            self.worker_thread = threading.Thread(target=self._connect_and_subscribe_thread, args=(subscriptions,), daemon=True)
            self.worker_thread.start()

    def disconnect(self):
        with self.lock:
            self.logger.info("WS_MANAGER: Disconnecting...", extra={'event': 'WS_MANAGER'})
            try:
                if self.client:
                    # Unsubscribing might not be possible if the subscribe call is blocking the thread.
                    # The primary way to stop is to disconnect.
                    self.client.disconnect()
                    self.logger.info("WS_MANAGER: Disconnected successfully.", extra={'event': 'WS_MANAGER'})
            except Exception as e:
                self.logger.error(f"WS_MANAGER: Error during disconnect: {e}", exc_info=True, extra={'event': 'ERROR'})
            finally:
                self.client = None
                if self.worker_thread and self.worker_thread.is_alive():
                    self.worker_thread.join(timeout=2)
                self.worker_thread = None

    def reconnect(self, subscriptions):
        self.logger.info("WS_MANAGER: Reconnect commanded.", extra={'event': 'WS_MANAGER'})
        self.disconnect()
        # Give a moment for resources to be released
        time.sleep(1)
        self.connect(subscriptions)

    def stop(self):
        self.is_running = False
        self.disconnect()
