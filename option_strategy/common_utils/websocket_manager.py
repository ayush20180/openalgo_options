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

        self._client = None
        self._worker_thread = None
        self._lock = threading.Lock()
        self.is_running = True

    def _connect_and_subscribe_thread(self, subscriptions, client_instance):
        """
        This function runs in a dedicated thread. It should not be locked for its whole duration.
        It operates on a client instance passed to it.
        """
        try:
            self.logger.info("WS_MANAGER: Worker thread started.", extra={'event': 'WS_MANAGER'})
            client_instance.connect()
            self.logger.info("WS_MANAGER: Connected successfully.", extra={'event': 'WS_MANAGER'})

            if subscriptions:
                self.logger.info(f"WS_MANAGER: Subscribing to {subscriptions}", extra={'event': 'WS_MANAGER'})
                # This call is blocking and will take over the thread until disconnect() is called from another thread.
                client_instance.subscribe_ltp(subscriptions, on_data_received=self.on_tick_callback)

            self.logger.info("WS_MANAGER: Subscription call returned, thread will exit.", extra={'event': 'WS_MANAGER'})

        except Exception as e:
            # Check if this is an expected disconnection
            if self.is_running:
                 self.logger.error(f"WS_MANAGER: Error in connection thread: {e}", exc_info=True, extra={'event': 'ERROR'})

        finally:
            with self._lock:
                # Clear the client instance if it's the current one
                if self._client is client_instance:
                    self._client = None
            self.logger.info("WS_MANAGER: Worker thread finished.", extra={'event': 'WS_MANAGER'})

    def connect(self, subscriptions):
        with self._lock:
            if self._worker_thread and self._worker_thread.is_alive():
                self.logger.warning("WS_MANAGER: connect called but a worker thread is already running.", extra={'event': 'WS_MANAGER'})
                return

            self.logger.info("WS_MANAGER: Starting connection thread.", extra={'event': 'WS_MANAGER'})
            # Create the client instance here, under the lock
            self._client = api(api_key=self.api_key, host=self.host, ws_url=self.ws_url)

            self._worker_thread = threading.Thread(target=self._connect_and_subscribe_thread, args=(subscriptions, self._client), daemon=True)
            self._worker_thread.start()

    def disconnect(self):
        thread_to_join = None
        client_to_disconnect = None

        with self._lock:
            self.logger.info("WS_MANAGER: Disconnecting...", extra={'event': 'WS_MANAGER'})
            if self._client:
                client_to_disconnect = self._client
                self._client = None

            if self._worker_thread and self._worker_thread.is_alive():
                thread_to_join = self._worker_thread
                self._worker_thread = None

        try:
            if client_to_disconnect:
                # This call should be outside the lock, as it may block or trigger actions in the worker thread.
                client_to_disconnect.disconnect()
                self.logger.info("WS_MANAGER: Disconnected successfully.", extra={'event': 'WS_MANAGER'})
        except Exception as e:
            self.logger.error(f"WS_MANAGER: Error during client disconnect: {e}", exc_info=True, extra={'event': 'ERROR'})

        if thread_to_join:
            thread_to_join.join(timeout=2.0)
            if thread_to_join.is_alive():
                self.logger.warning("WS_MANAGER: Worker thread did not exit in time.", extra={'event': 'WS_MANAGER'})

    def reconnect(self, subscriptions):
        self.logger.info("WS_MANAGER: Reconnect commanded.", extra={'event': 'WS_MANAGER'})
        self.disconnect()
        time.sleep(1) # Give a moment for resources to be released.
        self.connect(subscriptions)

    def stop(self):
        self.is_running = False
        self.disconnect()
