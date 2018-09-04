import requests
from bs4 import BeautifulSoup
import re
from langdetect import detect_langs
import pymysql
import logging


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)


generate_sql = "select url, url_id from result_url where visited=0"
after_generate_exist_sql = "update result_url set visited=1 where url=%s"
insert_data_sql = "insert into eng_result_url (url, text_data) values(%s, %s)"



def generate_url2():  # return generate urls
    connection = pymysql.connect(host='masternode', user='hduser', password='root', db='url_db', charset='utf8')
    with connection.cursor() as curs:
        curs.execute(generate_sql)
        generate_urls = curs.fetchmany(size=50)
        for url in generate_urls :
            curs.execute(after_generate_exist_sql,url[0])
        connection.commit()
        connection.close()
    return generate_urls  # need to tokenize ( generate_urls -> generate_url )

def parse_data2(generate_url):
    connection = pymysql.connect(host='masternode', user='hduser', password='root', db='url_db', charset='utf8')
    try:
        req = requests.get("http://" + generate_url,timeout=3)
        html = req.text
        soup = BeautifulSoup(html, 'html.parser',from_encoding='utf-8')
        urls = soup.find_all('a', href=True)
        data = ''

        for url in urls:
            if url.text != '' or url.text != ' ':
                data = data + url.text.replace('\n', '').replace('\r', '') + ' '

        data = re.sub('[^0-9a-zA-Z\\s\\.\\,]', '', data).lower()[:-1]
        data = re.sub('\\s+', ' ', data)[:1000]
        try:

            #result state
            # 0 : no language!
            # 1 : english!
            # 2 : other language!

            lang = detect_langs(data)[0].lang
            if lang == 'en':
                result =1
                connection.close()
                return data, result
            else:
                result =2
                connection.close()
                return data, result
        except:
            connection.close()
            result = 0
            return data, result
    except:
        logger.error("URL not exist : %s"%generate_url)
        connection.close()
        data =''
        result =0
        return data, result

def save_db2(origin_url,text_data):
    connection = pymysql.connect(host='masternode', user='hduser', password='root', db='url_db', charset='utf8')
    try:
        with connection.cursor() as curs:
            if text_data !="":
                curs.execute(insert_data_sql,(origin_url,text_data)) # insert url text data info.
                logger.info("Inserted : %s" % origin_url)
        connection.commit()
    finally:
        connection.close()


if __name__ == "__main__":
    generated = generate_url2()
    for origin_url in generated:
            data , result = parse_data2(origin_url[0])
            if result ==1 :
                save_db2(origin_url[0], data)