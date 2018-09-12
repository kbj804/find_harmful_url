# For find source_id

import pymysql
import time

find_source_id="select harmful_source_id from collected_url where url_id= %s"
verify_whitelist="select harmful from collected_url where url_id=%s"

update_node_whitelist = "UPDATE collected_url SET harmful='3' WHERE url_id= %s and harmful!='3'"
find_child_url="select child_id from url_relation where parent_id=%s"

load_whitelist ="select url_id from collected_url where harmful='3' and white_visit=0"
add_white_visit="update collected_url set white_visit = 1 where url_id=%s"


def extract_source(id):  #
    #connection = pymysql.connect(host='localhost', user='bj', password='1234', db='url_db', charset='utf8')
    with connection.cursor() as curs:
        curs.execute(find_source_id, id)
        for harmful_source_id in curs.fetchone(): # fetch -> dictionary
            if harmful_source_id == '0' :  # null
                return id
            else:
                id2 = extract_source(harmful_source_id)
                return id2

def update_white_list(id):
    with connection.cursor() as curs:
        curs.execute(find_source_id, id)
        for harmful_source_id in curs.fetchone():  # fetch -> dictionary
            if harmful_source_id == '0':  # null
                # connection.close()
                print("source id: ", id)
                return id

            else:
                curs.execute(find_child_url, id)
                child_group = curs.fetchall()
                print(child_group)
                for child_id in child_group:  ############################################ Need to solution duplication probelm
                    print("UPDATE WHITE LIST: ", child_id[0])
                    curs.execute(update_node_whitelist, child_id[0])

                # update child_node
                # curs.execute(update_node_whitelist, id)
                curs.close()
                id2 = extract_source(harmful_source_id)
                return id2


def verify(id):
    #connection = pymysql.connect(host='localhost', user='bj', password='1234', db='url_db', charset='utf8')
    with connection.cursor() as curs:
        curs.execute(verify_whitelist, id)
        for harmful in curs.fetchone(): # if id is in Whitelist, extract source id.
           # curs.close()
            if harmful == 3 :
                curs.execute(add_white_visit,id)
                print("verify id: ", id)
                curs.close()
                return 1
            else:
                print("It is harmful URL. ")
                curs.close()
                return 0


def generate_url():
    with connection.cursor() as curs:
        curs.execute(load_whitelist)
        white_list = curs.fetchall()
        curs.close()
        return white_list


if __name__ == "__main__":
    while 1:
        try:
            connection = pymysql.connect(host='localhost', user='bj', password='1234', db='url_db', charset='utf8')
            white_list = generate_url()
            for url_id in white_list:
                first_value = verify(url_id[0])  # value = 0: No Whitelist / value = 1: Whitelist
                if first_value == 1:
                    source_url = extract_source(url_id[0])
                    last_value = verify(source_url)
                    if last_value == 1 :
                        update_white_list(url_id[0])
                        print("Input All White list. ")
                else:
                    ("It is no whitelist...")
            connection.commit()
            connection.close()

        except:
            time.sleep(50)
            print("Maybe FINISH OR")
            print("EEEEEEEEEEEEEEEEEEEERRRRRRRRRRRRRRRRRRRRRRROOOOOOOOOOOOOOOOOOOOOOOORRRRRRRRRRRRRRRRRRRRROOOOOOOOOOOOOOOOOOOOOOOOO")