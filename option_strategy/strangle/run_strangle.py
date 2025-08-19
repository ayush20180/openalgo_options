import logging
import signal
import sys
from .strangle import StrangleStrategy

# Global strategy instance to be accessible by the signal handler
strategy = None

def graceful_shutdown(signal, frame):
    """Signal handler for Ctrl+C."""
    print("\nCtrl+C detected. Initiating graceful shutdown...")
    if strategy:
        # Assuming strategy has a logger configured
        strategy.logger.info("DIAGNOSTIC: Ctrl+C detected. Calling strategy.shutdown().", extra={'event': 'SHUTDOWN'})
        strategy.shutdown()
        strategy.logger.info("DIAGNOSTIC: strategy.shutdown() completed.", extra={'event': 'SHUTDOWN'})
    else:
        print("Strategy object not found. Exiting immediately.")
    print("Exiting.")
    sys.exit(0)

if __name__ == "__main__":
    """
    This is the main entry point to run the Strangle strategy.

    It initializes the strategy and starts its main execution loop.
    The script should be run from the root of the project, for example:
    `python -m option_strategy.strangle.run_strangle`
    """
    # Register the signal handler for Ctrl+C
    signal.signal(signal.SIGINT, graceful_shutdown)

    try:
        # The strategy name provided here must match the folder name
        # and the name used in config/state/log/trade files.
        strategy = StrangleStrategy(strategy_name="strangle")
        strategy.run()

    except FileNotFoundError as e:
        logging.basicConfig()
        logging.error(f"CRITICAL ERROR: Could not start strategy. Configuration file not found: {e}")
    except Exception as e:
        logging.basicConfig()
        logging.error(f"A critical, unhandled error occurred: {e}", exc_info=True)
