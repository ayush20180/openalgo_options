import logging
from abc import ABC, abstractmethod
from openalgo import api

class BaseStrategy(ABC):
    """
    An abstract base class for all option strategies. It defines the common
    structure and methods that every strategy must implement.
    """

    def __init__(self, strategy_config: dict, client: api, logger: logging.Logger):
        """
        Initializes the BaseStrategy.

        Args:
            strategy_config (dict): The configuration specific to this strategy.
            client (api): The OpenAlgo API client instance.
            logger (logging.Logger): The logger instance for this strategy.
        """
        self.strategy_config = strategy_config
        self.client = client
        self.logger = logger
        self.active_legs = {}  # To keep track of open positions for this strategy

    @abstractmethod
    def run(self):
        """
        The main entry point to start the strategy. This method will be
        called by the scheduler.
        """
        pass

    @abstractmethod
    def execute_entry(self):
        """
        Implements the logic for entering the initial position(s).
        """
        pass

    @abstractmethod
    def monitor_and_adjust(self):
        """
        Continuously monitors the open positions and performs adjustments
        based on the strategy's logic.
        """
        pass

    @abstractmethod
    def execute_exit(self, reason: str = "Scheduled exit"):
        """
        Implements the logic for exiting all open positions for the strategy.

        Args:
            reason (str): The reason for exiting the position.
        """
        pass
