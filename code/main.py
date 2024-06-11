import psycopg2
from prefect import task, flow, get_run_logger
from prefect.blocks.system import Secret

db_host = Secret.load("db-host").get()
db_name = Secret.load("db-name").get()
db_user = Secret.load("db-username").get()
db_password = Secret.load("db-password").get()

@task(retries=3, retry_delay_seconds=60, task_run_name="test-db-connection")
def test_database():
    logger = get_run_logger()
    try:
        connection = psycopg2.connect(
            host=db_host,
            port=5432,
            database=db_name,
            user=db_user,
            password=db_password
        )
        cursor = connection.cursor()
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()
        logger.info(f"PostgreSQL database version: {db_version}")  
        cursor.close()
        connection.close()
    except psycopg2.Error as error:
        logger.error(f"Error connecting to PostgreSQL database: {error}")

@flow(name="my-flow", retries=3, retry_delay_seconds=60)
def test():
    test_database()