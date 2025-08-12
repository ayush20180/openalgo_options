from apscheduler.schedulers.background import BackgroundScheduler
from pytz import utc, timezone

def init_scheduler():
    """
    Initializes and returns a BackgroundScheduler instance configured for the
    IST timezone.
    """

    # Configure the scheduler
    # The 'apscheduler.timezone' setting ensures that the scheduler interprets
    # the start and end times in the correct timezone.
    scheduler_config = {
        'apscheduler.timezone': 'Asia/Kolkata',
    }

    scheduler = BackgroundScheduler(job_defaults={'misfire_grace_time': 60}, **scheduler_config)

    return scheduler
