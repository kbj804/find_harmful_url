from celery import Celery
import requests
from crawl_test2 import parse_data, save_db, logger
from findurl import find_url, work2
from data_crawl import parse_data2, save_db2

###app= Celery('tasks',broker="redis://masternode")
###app.conf.CELERY_RESULT_BACKEND='db+mysql+pymysql://hduser:root@masternode/url'

@app.task
def jkcrawl_task(generated):
    try:
        parsed, text_data, result = parse_data(generated[0])
    except:
        logger.info("some parse_data error occured.")
    try:
        save_db(generated[1], text_data, parsed, result)
    except:
        logger.info("some save_db error occured.")


@app.task
def jkfind_task(id):
        work2(id)

@app.task
def jktext_task(generated):
    try:
        data, result = parse_data2(generated[0])
    except:
        logger.info("some parse_data error occured.")
    try:
        if result == 1:
            save_db2(generated[0], data)
        else :
            logger.info("no english web site!")
    except:
        logger.info("some save_db error occured.")