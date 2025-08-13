import json
import os

class StateManager:
    """
    Manages the state of a strategy by reading and writing it to a JSON file.
    This allows a strategy to be stateless and resume from where it left off
    after a restart.
    """

    def __init__(self, state_path: str):
        """
        Initializes the StateManager.

        Args:
            state_path (str): The directory where state files should be stored.
        """
        self.state_path = state_path
        os.makedirs(self.state_path, exist_ok=True)

    def _get_state_file_path(self, strategy_name: str) -> str:
        """Constructs the full path for a strategy's state file."""
        return os.path.join(self.state_path, f"{strategy_name}_state.json")

    def load_state(self, strategy_name: str) -> dict:
        """
        Loads the last known state for a given strategy.

        Args:
            strategy_name (str): The name of the strategy.

        Returns:
            dict: The loaded state, or an empty dictionary if no state file is found.
        """
        state_file = self._get_state_file_path(strategy_name)
        if not os.path.exists(state_file):
            return {}  # Return an empty state if the file doesn't exist

        try:
            with open(state_file, 'r') as f:
                state = json.load(f)
            return state
        except (json.JSONDecodeError, IOError) as e:
            # Handle cases where the file is corrupted or unreadable
            print(f"Warning: Could not load state file for {strategy_name}: {e}")
            return {}

    def save_state(self, strategy_name: str, state_data: dict):
        """
        Saves the current state for a given strategy.

        Args:
            strategy_name (str): The name of the strategy.
            state_data (dict): The current state data to save.

        Returns:
            bool: True if saving was successful, False otherwise.
        """
        state_file = self._get_state_file_path(strategy_name)
        try:
            with open(state_file, 'w') as f:
                json.dump(state_data, f, indent=4)
            return True
        except IOError as e:
            print(f"Error: Could not save state file for {strategy_name}: {e}")
            return False
