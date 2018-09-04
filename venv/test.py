# Input url to url_realtion Table

import pymysql


# start_sql="select url_id from collected_url where url_id=%s"
#find_child="select child_id from url_relation where parent_id=%s and harmhul=2"

rotation_input="select url from collected_url where url_id=%s"
update_relation="update url_relation set url=%s where parent_id=%s"


def find_last_child(url_id):
    connection = pymysql.connect(host='localhost',
                                 user='bj',
                                 password='1234',
                                 db='url_db',
                                 charset='utf8')

    try:  # do
        with connection.cursor() as curs:

            curs.execute(find_child,url_id)#
            first_child = curs.fetchall()
            print(first_child,url_id)
            for id in first_child:
                curs.execute(find_child,id)
                second_child = curs.fetchall()
                print(second_child)

            # (url_id) ...
    finally:
        connection.close()

def generate_url(num):  # return generate urls
    connection = pymysql.connect(host='localhost', user='bj', password='1234', db='url_db', charset='utf8')
    with connection.cursor() as curs:
        curs.execute(rotation_input,num)
        generate_urls = curs.fetchone()
        print(generate_urls,num)
        connection.commit()
        connection.close()
    return generate_urls  # need to tokenize ( generate_urls -> generate_url )


def update(url_id, url):
    connection = pymysql.connect(host='localhost', user='bj', password='1234', db='url_db', charset='utf8')
    with connection.cursor() as curs:
        print(url,url_id)
        curs.execute(update_relation, (url, url_id))
        connection.commit()
        connection.close()

def exe():
    for i in range(2, 100000):
        generated = generate_url(i)
        update(i, generated)


if __name__ == "__main__":
    exe()
#
#    try:
#        for i in range(1,100000):
#            generated = generate_url(i)
#            update(i, generated)
#
#    except:
#       print("ERROR")
