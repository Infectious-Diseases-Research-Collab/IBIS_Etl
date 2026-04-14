from __future__ import annotations

import logging
import os

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL

logger = logging.getLogger(__name__)

SCHEMAS = ['bronze_ibis', 'silver_ibis', 'gold_ibis', 'ibis', 'store_ibis']


def create_db_engine(config) -> Engine:
    """Create a SQLAlchemy engine from the 'db' config block.
    Uses URL.create() to safely handle special characters in the password.
    """
    db = config.get('db')
    password = os.environ[db['password_env']]
    url = URL.create(
        drivername='postgresql+psycopg2',
        username=db['user'],
        password=password,
        host=db['host'],
        port=db['port'],
        database=db['name'],
    )
    return create_engine(url, pool_pre_ping=True)


def init_schemas(engine: Engine) -> None:
    """Create all medallion schemas if they do not already exist."""
    with engine.connect() as conn:
        for schema in SCHEMAS:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {schema}'))
            logger.debug('Schema ready: %s', schema)
        conn.commit()
    logger.info('Initialised schemas: %s', SCHEMAS)
