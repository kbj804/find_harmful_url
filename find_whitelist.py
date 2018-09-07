# For find source_id

import pymysql

find_source_id="select harmful_source_id from collected_url where url_id= %s"
verify_whitelist="select harmful from collected_url where url_id=%s"



def extract_source(id):  #
    #connection = pymysql.connect(host='localhost', user='bj', password='1234', db='url_db', charset='utf8')
    with connection.cursor() as curs:
        curs.execute(find_source_id, id)
        for harmful_source_id in curs.fetchone(): # fetch -> dictionary
            if harmful_source_id == '0' :  # null
                connection.close()
                print("source id: ", id)
                return id
            else:
                print(harmful_source_id)
                id2 = extract_source(harmful_source_id)
                return id2


def verify(id):
    #connection = pymysql.connect(host='localhost', user='bj', password='1234', db='url_db', charset='utf8')
    with connection.cursor() as curs:
        curs.execute(verify_whitelist, id)
        for harmful in curs.fetchone(): # if id is in Whitelist, extract source id.
            if harmful == 3 :
                connection.close()
                print("start id: ", id)
                source_url = extract_source(id)
                return 1
            else:
                print("It is harmful URL. ")
                return 0



if __name__ == "__main__":
    connection = pymysql.connect(host='localhost', user='bj', password='1234', db='url_db', charset='utf8')
    verify(16085)  # input url_id
