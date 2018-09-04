import pymysql.cursors
import logging
import time

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)



count_query = "select count(parent_id) from url_relation as A inner join " \
                    " (select url_id from collected_url where harmful='1' or harmful='2') as B " \
                    "where A.child_id = B.url_id and harmful='0' ";

select_query = "select parent_id,child_id from url_relation as A inner join " \
                "(select url_id from collected_url where harmful='1' or harmful='2') as B  " \
                "where A.child_id = B.url_id and harmful ='0' ";


result_select_query = "select url,country,harmful from collected_url where url_id = %s"
result_en_query = "select text_data from en_data where url_id = %s"
result_another_query = "select text_data from another_data where url_id = %s"

harmful_update_query = "update collected_url set harmful=2, harmful_source_id= %s where url_id= %s"
add_harmful_count_sql="update collected_url set harmful_count = harmful_count+1 where url_id=%s"

#delete_query = "delete from url_relation where parent_id= %s "
replace_delete_query="update url_relation set harmful=2 where child_id=%s"
insert_query = "insert into result_url (url, text_data, country) values (%s, %s, %s)"



def find_url():
    connection = pymysql.connect(host='localhost',
                                     user='bj',
                                     password='1234',
                                     db='url_db',
                                     charset='utf8')

    try:  # do
        with connection.cursor() as curs:
            curs.execute(select_query)
            result = curs.fetchall() # (parent_id, child_id) ...
            print(len(result))
            connection.close()
        for id in result:
            work2(id)



    finally:
        with connection.cursor() as curs:
            curs.execute(count_query)
            end = curs.fetchone()

        if ( end[0] == 0 ) : #while
            connection.close()



def work2(id): # id[0] = parent_id, id[1] = child_id
    connection = pymysql.connect(host='localhost',
                                 user='bj',
                                 password='1234',
                                 db='url_db',
                                 charset='utf8')

    try:
        with connection.cursor() as curs:
            curs.execute(result_select_query, id[0])
            url, lang, harmful = curs.fetchone()


            text_data = ""
            if curs.execute(result_en_query, id[0]) != 0 or curs.execute(result_another_query, id[0]) != 0:
                # case1: site had text_data
                text_data = curs.fetchone()[0]

            if (harmful > 0):
                # already this site is harmful. (harmful = 1 or 2 )
                curs.execute(add_harmful_count_sql, id[0])
                curs.execute(replace_delete_query, id[1])
            else:
                # ADD NEW HARMFUL SITE with source_id
                logger.info("Harmful Site : %s" % url)
                curs.execute(harmful_update_query, (id[1], id[0]))
                curs.execute(replace_delete_query, id[1])
                #curs.execute(delete_query,id)
                curs.execute(insert_query,(url, text_data, lang))
            connection.commit()
    finally:
        connection.close()


if __name__ == "__main__":
    while 1:

        try:
            find_url()
        except:
            logger.error('FINISH OR ERROR')
            time.sleep(100)