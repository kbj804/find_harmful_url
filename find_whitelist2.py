# For find source_id

import pymysql
import time

verify_whitelist= "SELECT a.child_id FROM url_db.url_relation as a inner join"\
	                    "(select url_id,url,harmful,harmful_source_id"\
	                    " from collected_url"\
                        " where harmful='1' or harmful='2'"\
                        "  ) as b"\
                " where a.child_id = b.url_id and a.parent_id = %s"

update_node_whitelist = "UPDATE collected_url SET harmful='3' WHERE url_id= %s and harmful!='3'"

load_whitelist ="select A.url_id, A.harmful_source_id from collected_url as A inner join ( " \
	                        " select url_id from collected_url  where harmful = '3' and white_visit = 0 ) as B " \
                "where A.harmful_source_id = B.url_id and harmful = '2' ";

# add_white_visit="update collected_url set white_visit = 1 where url_id=%s"




def TempChild_verify(id):
    #connection = pymysql.connect(host='localhost', user='bj', password='1234', db='url_db', charset='utf8')
    with connection.cursor() as curs:
        curs.execute(verify_whitelist, id)
        harmful_id = curs.fetchall()
        if harmful_id:
            print("harmful: ", harmful_id)
            return 0
        else:
            curs.execute(update_node_whitelist ,id)
            print(id, "'s WhiteList Right")
            return 1



def generate_url():
    with connection.cursor() as curs:
        curs.execute(load_whitelist)
        white_list = curs.fetchall()
        print(len(white_list))
        curs.close()
        return white_list


if __name__ == "__main__":
    while 1:
        try:
            connection = pymysql.connect(host='localhost', user='bj', password='1234', db='url_db', charset='utf8')
            white_list = generate_url()
            for temp_id in white_list:
                print(temp_id)
                harmful_temp_id, harmful_source_id = temp_id
                if TempChild_verify(harmful_temp_id) == 1:
                    print("Input WhiteList and Update <harmful and white_visit) ")
                else:
                    print(" Pass Next Temp_harmful ID ")

            time.sleep(1)
            connection.commit()
            connection.close()

        except:
            time.sleep(5)
            print("Maybe FINISH OR")
            print("EEEEEEEEEEEEEEEEEEEERRRRRRRRRRRRRRRRRRRRRRROOOOOOOOOOOOOOOOOOOOOOOORRRRRRRRRRRRRRRRRRRRROOOOOOOOOOOOOOOOOOOOOOOOO")