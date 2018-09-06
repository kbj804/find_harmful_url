# For find source_id

import pymysql

find_source_id="select harmful_source_id from collected_url where url_id=%s"


def find_source(id):  # return generate urls
    connection = pymysql.connect(host='localhost', user='bj', password='1234', db='url_db', charset='utf8')
    with connection.cursor() as curs:
        curs.execute(find_source_id, id)
        harmful_source_id = curs.fetchone()
        print(harmful_source_id)

        if harmful_source_id is None: # null
            connection.close()
            return id
        elif harmful_source_id is not None:
            find_source(harmful_source_id)



def exe(id):
        source_id = find_source(id)
        print("source")


if __name__ == "__main__":
    exe(25253)  # input url_id
