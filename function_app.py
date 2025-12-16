import datetime
import logging
import azure.functions as func

try:
    import psycopg  # psycopg v3
except ImportError:
    psycopg = None

app = func.FunctionApp()

@app.function_name(name="mytimer")
@app.timer_trigger(
    schedule="0 */5 * * * *",
    arg_name="mytimer",
    run_on_startup=False,
)
def test_function(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.warning("The timer is past due!")

    logging.info("Timer ran at %s", utc_timestamp)
    logging.info("psycopg installed: %s", psycopg is not None)

    if psycopg is not None:
        logging.info("psycopg version: %s", getattr(psycopg, "__version__", "unknown"))
