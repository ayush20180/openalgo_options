import time
from datetime import datetime, timedelta

class MockOpenAlgoClient:
    """
    A mock client that simulates the OpenAlgo API for testing purposes.
    It returns hardcoded, realistic data without making any network requests.
    """

    def __init__(self, api_key: str, host: str):
        self.api_key = api_key
        self.host = host

    def expiry(self, symbol: str, exchange: str, instrumenttype: str):
        """Mocks the expiry API endpoint."""
        # Return a list of plausible future expiry dates
        today = datetime.now()
        expiries = []
        for i in range(1, 5): # Next 4 weeks
            thursday = today + timedelta(days=((3 - today.weekday() + 7) % 7) + (i-1)*7)
            expiries.append(thursday.strftime('%d-%b-%y'))

        # Add a monthly expiry
        next_month = today.replace(day=28) + timedelta(days=4)
        last_day = next_month - timedelta(days=next_month.day)
        while last_day.weekday() != 3: # Find last Thursday
            last_day -= timedelta(days=1)
        expiries.append(last_day.strftime('%d-%b-%y'))

        return {"status": "success", "data": sorted(list(set(expiries)))}

    def quotes(self, symbol: str, exchange: str):
        """Mocks the quotes API endpoint."""
        # Return a realistic quote based on the symbol
        if "INDEX" in exchange: # For index spot price
            return {"status": "success", "data": {"ltp": 22500.0}}

        # For options, return a dummy premium
        price = 100.0
        if "22700CE" in symbol:
            price = 150.0
        elif "22300PE" in symbol:
            price = 120.0

        return {"status": "success", "data": {"ltp": price, "oi": 100000}}

    def placeorder(self, strategy: str, symbol: str, action: str, exchange: str, price_type: str, product: str, quantity: int, **kwargs):
        """Mocks the placeorder API endpoint."""
        return {"status": "success", "data": {"order_id": f"mock_{int(time.time())}"}}

    # Mock other methods as needed for more complex tests
    def search(self, query: str, exchange: str):
        pass

    def closeposition(self, strategy: str):
        pass
