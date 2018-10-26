# change WhiteList's Child -> WhiteList

import pymysql
import time

find_source_id="select harmful_source_id from collected_url where url_id= %s"
verify_whitelist= "SELECT a.child_id FROM url_db.url_relation as a inner join"\
	                    "(select url_id,url,harmful,harmful_source_id"\
	                    " from collected_url"\
                        " where harmful='0' or harmful='2'"\
                        "  ) as b"\
                " where a.child_id = b.url_id and a.parent_id = %s "

fix_whitelist = "UPDATE collected_url SET harmful='3' WHERE url_id= %s and harmful!='3'"
find_child_url="select child_id from url_relation where parent_id=%s"

load_whitelist ="select url_id from collected_url where harmful='3' and white_visit=0"

add_white_visit="update collected_url set white_visit = 1 where url_id=%s"




def TempChild_verify(id):
    #connection = pymysql.connect(host='localhost', user='bj', password='1234', db='url_db', charset='utf8')
    with connection.cursor() as curs:
        curs.execute(verify_whitelist, id)
        harmful_id = curs.fetchall()
        if harmful_id or id != 19111 or id != 21565 or id !=21562:
            print("Parents ID: ",id)
            for change_id in harmful_id:
                curs.execute(fix_whitelist, change_id[0])
                print("Add Child ID: ", change_id[0] )
            return 0

        else:
            curs.execute(add_white_visit, id)
            print(id, "' ADD WHITE VISIT")
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
                #TempChild_verify(temp_id[0])
               # print(temp_id)
                if TempChild_verify(temp_id) == 1:
                    print("Input WhiteList and Update <harmful and white_visit) ")
                else:
                    print("Change child's harmful")

            time.sleep(1)
            connection.commit()
            connection.close()

        except:
            time.sleep(5)
            print("Maybe FINISH OR")
            print("EEEEEEEEEEEEEEEEEEEERRRRRRRRRRRRRRRRRRRRRRROOOOOOOOOOOOOOOOOOOOOOOORRRRRRRRRRRRRRRRRRRRROOOOOOOOOOOOOOOOOOOOOOOOO")