import logging
import pymysql.cursors
import redis
import time

from celery import Celery

from tasks import jkcrawl_task, jkfind_task, jktext_task
from crawl_test2 import generate_url
from data_crawl import generate_url2


count_query = "select distinct count(parent_id) from url_relation as A inner join " \
                    " (select url_id from collected_url where harmful='1' or harmful='2') as B " \
                    "where A.child_id = B.url_id";

select_query = "select distinct parent_id from url_relation as A inner join " \
                "(select url_id from collected_url where harmful='1' or harmful='2') as B  " \
                "where A.child_id = B.url_id";



#logger configuration
###app = Celery('tasks',broker="redis://masternode")
###app.conf.CELERY_RESULT_BACKEND='db+mysql+pymysql://hduser:root@masternode/url'

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)

def manage_jkcrawl_task(generated):
    for url in generated:
        jkcrawl_task.apply_async((url,),queue='jkcrawler_queue')

def manage_jkfind_task(result):
    for id in result:
        jkfind_task.apply_async((id,),queue='jkfind_queue')

def manage_jktext_task(generated):
    for url in generated:
        jktext_task.apply_async((url,), queue='jktext_queue')

if __name__ =='__main__':
    # when crawl tasks
    while 1:
        try:
            generated=generate_url()
            manage_jkcrawl_task(generated)
            time.sleep(1)
        except:
            logger.error('Deadlock has occured.')



    #when crawl english result text data
    # while 1:
    #     try:
    #         generated = generate_url2()
    #         if generated is None:
    #             break
    #         manage_jktext_task(generated)
    #     except:
    #         logger.error('Deadlock has occured.')


    #when find tasks
    #
    # redis_client = redis.Redis(host='masternode', port=6379)
    # while 1:
    #     queue_length = 0
    #     queue_length = redis_client.llen('jkfind_queue')
    #     while queue_length != 0:
    #         queue_length = redis_client.llen('jkfind_queue')
    #         time.sleep(5)
    #
    #     logger.info("jkfind_queue is Empty. Begin find tasks again.")
    #
    #     connection = pymysql.connect(host='masternode', user='hduser', password='root', db='url_db2', charset='utf8')
    #     with connection.cursor() as curs:
    #         curs.execute(select_query)
    #         result = curs.fetchall()
    #
    #     manage_jkfind_task(result)
    #
    #     with connection.cursor() as curs:
    #         curs.execute(count_query)
    #         end = curs.fetchone()
    #
    #     if ( end[0] == 0 ) : #while
    #         connection.close()
    #         break
    #


