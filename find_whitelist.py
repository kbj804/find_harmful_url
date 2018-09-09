# For find source_id

import pymysql

find_source_id="select harmful_source_id from collected_url where url_id= %s"
verify_whitelist="select harmful from collected_url where url_id=%s"

update_node_whitelist = "UPDATE collected_url SET harmful='3' WHERE url_id= %s"
find_child_url="select child_id from url_relation where parent_id=%s"

def extract_source(id):  #
    #connection = pymysql.connect(host='localhost', user='bj', password='1234', db='url_db', charset='utf8')
    with connection.cursor() as curs:
        curs.execute(find_source_id, id)
        for harmful_source_id in curs.fetchone(): # fetch -> dictionary
            if harmful_source_id == '0' :  # null
                #connection.close()
                print("source id: ", id)
                return id

            else:
                curs.execute(find_child_url, id)
                child_group = curs.fetchall()
                print(child_group)
                for child_id in child_group: ############################################ Need to solution duplication probelm
                    print("UPDATE WHITE LIST: ", child_id[0])
                    wtf=curs.execute(update_node_whitelist,child_id[0])
                    print(wtf)

                # update child_node
                # curs.execute(update_node_whitelist, id)

                id2 = extract_source(harmful_source_id)
                return id2



def verify(id):
    #connection = pymysql.connect(host='localhost', user='bj', password='1234', db='url_db', charset='utf8')
    with connection.cursor() as curs:
        curs.execute(verify_whitelist, id)
        for harmful in curs.fetchone(): # if id is in Whitelist, extract source id.
            if harmful == 3 :
                #connection.close()
                print("verify id: ", id)
                return 1
            else:
                print("It is harmful URL. ")
                return 0



if __name__ == "__main__":
    connection = pymysql.connect(host='localhost', user='bj', password='1234', db='url_db', charset='utf8')
    url_id = 16085
    first_value = verify(url_id)  # value = 0: No Whitelist / value = 1: Whitelist
    if first_value == 1:
        source_url = extract_source(url_id)
        last_value = verify(source_url)
        if last_value == 1 :
            connection.commit()
            connection.close()
            print("Input All White list. ")
    else:
        ("else")