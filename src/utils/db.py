import psycopg
from utils.settings import get_settings
from pydantic_settings import BaseSettings
from utils.logger import get_logger


log = get_logger()
settings = get_settings()
def get_connection(db_set: BaseSettings = settings) -> psycopg.Connection:
    try:
        db_con = psycopg.connect(f"postgresql://{db_set.DB_USER}:{db_set.DB_PASSWORD}@{db_set.DB_HOST}:{db_set.DB_PORT}/{db_set.DB_NAME}")
        log.info("Connection to DB has been created. State: ", db_con.__getstate__)
        return db_con
    except Exception as e:
        log.error("Connection to DB failed: ", e)
        