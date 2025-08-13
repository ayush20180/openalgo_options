import os
import logging
import asyncio
import re
import time
from datetime import datetime
import pandas as pd
import json
from dotenv import load_dotenv

# It's better to use the existing httpx client for making API calls
# as it's likely already configured in the main app.
# However, per user instructions, this is a standalone script.
# We will use 'requests' for simplicity here if httpx is complex to set up standalone.
import requests

class BaseStrategy:
    """
    A base class for standalone, time-driven strategies.
    It contains common functionalities like API client setup, logging,
    and reusable methods for order placement and data fetching.
    """

    def __init__(self, config_path: str, paper_trade_log_path: str):
        """
        Initializes the BaseStrategy.

        Args:
            config_path (str): Path to the JSON config file.
            paper_trade_log_path (str): Path to the CSV file for logging paper trades.
        """
        load_dotenv(override=True)

        self.api_key = os.getenv("APP_KEY")
        self.host_server = os.getenv("HOST_SERVER")

        if not self.api_key or not self.host_server:
            raise ValueError("APP_KEY and HOST_SERVER must be set in .env file")

        with open(config_path, 'r') as f:
            self.config = json.load(f)

        self.paper_trade_log_path = paper_trade_log_path
        self._setup_logging()
        self._sym_rx = re.compile(r"^[A-Z]+(\d{2}[A-Z]{3}\d{2})(\d+)(CE|PE)$")
        self.active_legs = {}

    def _setup_logging(self):
        """Sets up a logger for the strategy."""
        self.logger = logging.getLogger(self.config.get("strategy_name", "BaseStrategy"))
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        if not self.logger.handlers:
            self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    def _make_api_request(self, method: str, endpoint: str, payload: dict = None):
        """A centralized method for making API requests."""
        url = f"{self.host_server}/api/v1/{endpoint}"
        req_payload = payload if payload else {}
        req_payload['apikey'] = self.api_key

        try:
            if method.upper() == 'POST':
                response = requests.post(url, json=req_payload, timeout=10)
            else:
                response = requests.get(url, params=req_payload, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request to {url} failed: {e}")
            return {"status": "error", "message": str(e)}

    def get_expiry_dates(self, symbol: str, exchange: str, instrument_type: str = 'options'):
        """Fetches available expiry dates."""
        return self._make_api_request('POST', 'expiry', {
            "symbol": symbol,
            "exchange": exchange,
            "instrumenttype": instrument_type
        })

    def get_quote(self, symbol: str, exchange: str):
        """Fetches a quote for a given symbol."""
        return self._make_api_request('POST', 'quotes', {
            "symbol": symbol,
            "exchange": exchange
        })

    def place_order(self, symbol: str, action: str, quantity: int, product_type: str, exchange: str):
        """Places a market order."""
        # This is a simplified place_order. The real API has more fields.
        # This function can be expanded as needed.
        return self._make_api_request('POST', 'placeorder', {
            "symbol": symbol,
            "action": action,
            "quantity": str(quantity), # Server expects quantity as a string
            "product": product_type,
            "exchange": exchange,
            "pricetype": "MARKET", # Correct key is 'pricetype' not 'price_type'
            "strategy": self.config.get("strategy_name")
        })

    def log_paper_trade(self, symbol: str, action: str, quantity: int, price: float):
        """Logs a trade to the paper trading CSV file."""
        log_entry = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'strategy_name': self.config.get("strategy_name"),
            'mode': 'PAPER',
            'symbol': symbol,
            'action': action,
            'quantity': quantity,
            'price': price,
            'order_status': 'FILLED'
        }
        log_df = pd.DataFrame([log_entry])

        # Check if file exists to write header
        file_exists = os.path.isfile(self.paper_trade_log_path)
        log_df.to_csv(self.paper_trade_log_path, mode='a', header=not file_exists, index=False)
        self.logger.info(f"Logged PAPER trade: {action} {quantity} of {symbol} at {price}")
