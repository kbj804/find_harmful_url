from bs4 import BeautifulSoup
import requests
from urllib.parse import urlparse
import pymysql
import logging
from tld import get_tld


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)

dup_sql="select url from test_url where url=%s"
insert_sql= "insert into test_url (url, visited) values(%s, %s)"
generate_sql = "select url from test_url where visited=0"
after_gernerate_sql = "update test_url set visited=1 where url=%s"

def generate_url(): # return generate urls
    connection = pymysql.connect(host='masternode', user='hduser', password='root', db='url_db', charset='utf8')
    with connection.cursor() as curs:
        curs.execute(generate_sql)
        generate_urls = curs.fetchmany(size=50) # we need to change to fetchmany()
        curs.executemany(after_gernerate_sql,(generate_urls))
        connection.commit()
        connection.close()
    return generate_urls # need to tokenize ( generate_urls -> generate_url )

def parse_data(generate_url):
    try :
        req = requests.get("http://"+generate_url,timeout=5) # need to modify to adapt "http or https"
        html = req.text
        
        soup = BeautifulSoup(html,'html.parser')
        urls = soup.find_all('a',href=True)
        urls_set =set() # make set to remove duplication.
        for url in urls:
            str = urlparse(url['href']).netloc # by using urlparse, we can check it is http:// or https:// etc.
            if str!="":
                urls_set.add(get_tld("http://"+str.replace(" ","")))
            #if str.find('www') != -1:
            #    urls_set.add(str[4:].replace(" ",""))
            #else:
            #    urls_set.add(str.replace(" ",""))
        try :
            urls_set.remove("")
        finally:
            return urls_set
    except:
        logger.error("HTTP GET ERROR")


def save_db(urls_set):
    #sould use 'try-finally' and 'with' because connection to db can cause connection leak when it raise exceptions.
    connection = pymysql.connect(host='masternode', user='hduser', password='root', db='url_db', charset='utf8')
    try:
        with connection.cursor() as curs :
            for url in urls_set:
                if curs.execute(dup_sql,(url))==0: #when new url is not in database.
                    curs.execute(insert_sql,(url, 0))
                    logger.info("Inserted : " + url)
                else :
                    logger.info("Duplicated : "+ url)
        connection.commit()
    finally:
        connection.close()

if __name__ == "__main__":
    generated = generate_url()
    for i in generated:
        try:
            parsed = parse_data(i[0])
            save_db(parsed)
        except:
            logger.error("Connection Error")