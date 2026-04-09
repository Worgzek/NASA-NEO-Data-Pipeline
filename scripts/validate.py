import pandas as pd
import os
import sys
from loguru import logger
from db_log import log_ETL

run_date = sys.argv[1]
LOG_FILE = f"/opt/airflow/logs/pipeline_{run_date}.log"
logger.remove()
logger.add(sys.stdout, level='INFO')
logger.add(
    LOG_FILE,
    rotation='7 days',
    level='INFO',
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
)

def validate(run_date):
    df = pd.read_csv(f"/opt/airflow/data/processed/asteroids_{run_date}.csv")
    logger.info("dang doc file csv")

    duplicated = df[df.duplicated(subset=['gitasteroid_id', 'date'])]
    logger.info(f"da phat hien {len(duplicated)} dong bi duplicate")
    df = df.drop_duplicates(subset=['asteroid_id', 'date'])

    null = df.isnull().sum()
    null = null[null>0]
    if len(null) > 0:
        logger.info(f"da phat hien {null} thong tin bi null")
    else:
        logger.info('khong phat hien null')