from .strangle import StrangleStrategy

if __name__ == "__main__":
    # Define paths relative to this script's location
    # Note: This assumes you run this script as a module from the project root,
    # e.g., `python -m option_strategy.run_strangle`
    CONFIG_PATH = 'option_strategy/config.json'
    LOG_PATH = 'option_strategy/paper_trades.csv'

    try:
        # Initialize the strategy with its configuration
        strategy = StrangleStrategy(
            config_path=CONFIG_PATH,
            paper_trade_log_path=LOG_PATH
        )

        # Run the strategy's main execution loop
        strategy.run()

    except FileNotFoundError:
        print(f"Error: Configuration file not found at {CONFIG_PATH}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
