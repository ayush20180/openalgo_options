import os
import logging
from dotenv import load_dotenv
from flask import Flask, jsonify

from openalgo import api
from core.scheduler import init_scheduler
from core.mock_openalgo_client import MockOpenAlgoClient
from option_strategy.strangle import StrangleStrategy

# --- Basic Setup ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("OpenAlgoFramework")

# --- Flask App Initialization ---
app = Flask(__name__)

# --- Global Components ---
try:
    API_KEY = os.getenv("APP_KEY")
    HOST_SERVER = os.getenv("HOST_SERVER")
    APP_ENV = os.getenv("APP_ENV", "production")

    if not API_KEY or not HOST_SERVER:
        raise ValueError("API_KEY and HOST_SERVER must be set in .env file")

    if APP_ENV == 'development':
        logger.info("Application starting in DEVELOPMENT mode with Mock API Client.")
        client = MockOpenAlgoClient(api_key=API_KEY, host=HOST_SERVER)
    else:
        logger.info("Application starting in PRODUCTION mode with Live API Client.")
        client = api(api_key=API_KEY, host=HOST_SERVER)

    scheduler = init_scheduler()
    logger.info("Scheduler and API client initialized.")
except Exception as e:
    logger.error(f"Failed to initialize components: {e}", exc_info=True)
    client = None
    scheduler = None

def load_and_schedule_strategies():
    """
    Finds, loads, and schedules all enabled strategies.
    For now, it's hardcoded for StrangleStrategy.
    """
    if not client or not scheduler:
        logger.error("Client or Scheduler not available. Cannot schedule strategies.")
        return

    # In the future, this could dynamically discover strategies
    # from the option_strategy folder.
    strangle_config_path = 'config/strangle_config.json'

    try:
        strategy_instance = StrangleStrategy(
            strategy_config_path=strangle_config_path,
            client=client,
            logger=logging.getLogger("StrangleStrategy")
        )

        config = strategy_instance.strategy_config
        if not config.get('enabled', False):
            logger.info(f"Strategy '{config['strategy_name']}' is disabled. Skipping.")
            return

        logger.info(f"Scheduling strategy: {config['strategy_name']}")

        start_h, start_m = map(int, config['start_time'].split(':'))
        end_h, end_m = map(int, config['end_time'].split(':'))

        # Schedule entry
        scheduler.add_job(
            strategy_instance.execute_entry,
            'cron',
            day_of_week='mon-fri',
            hour=start_h,
            minute=start_m,
            id=f"{config['strategy_name']}_entry"
        )
        logger.info(f"Entry for '{config['strategy_name']}' scheduled at {config['start_time']}.")

        # Schedule monitoring
        scheduler.add_job(
            strategy_instance.monitor_and_adjust,
            'interval',
            minutes=1,
            start_date=f"2024-01-01 {config['start_time']}:00", # Dummy date
            end_date=f"2024-01-01 {config['end_time']}:00",   # Dummy date
            id=f"{config['strategy_name']}_monitor"
        )
        logger.info(f"Monitoring for '{config['strategy_name']}' scheduled every 1 minute between start and end times.")

        # Schedule exit if intraday
        if config.get('run_intraday', False):
            scheduler.add_job(
                strategy_instance.execute_exit,
                'cron',
                day_of_week='mon-fri',
                hour=end_h,
                minute=end_m,
                id=f"{config['strategy_name']}_exit"
            )
            logger.info(f"Intraday exit for '{config['strategy_name']}' scheduled at {config['end_time']}.")

    except Exception as e:
        logger.error(f"Failed to load or schedule strategy from {strangle_config_path}: {e}", exc_info=True)


@app.route('/status', methods=['GET'])
def status():
    """A simple endpoint to check scheduler status."""
    if not scheduler:
        return jsonify({"status": "error", "message": "Scheduler not initialized."}), 500

    jobs = []
    for job in scheduler.get_jobs():
        jobs.append({
            "id": job.id,
            "name": job.name,
            "trigger": str(job.trigger),
            "next_run_time": str(job.next_run_time)
        })
    return jsonify({"scheduler_status": "running", "jobs": jobs})

if __name__ == "__main__":
    load_and_schedule_strategies()
    if scheduler:
        scheduler.start()
        logger.info("Scheduler started. Starting Flask app.")
    else:
        logger.error("Could not start scheduler.")

    # Running Flask app in debug mode can cause scheduler to run jobs twice.
    # Use use_reloader=False to prevent this.
    app.run(host='0.0.0.0', port=5001, use_reloader=False)
