import requests
import re
from bs4 import BeautifulSoup
from tld import get_tld
from urllib.parse import urlparse
from tldextract import extract
# req = requests.get("http://www.pornonext.com/")
# html = req.text
#
# soup = BeautifulSoup(html,'html.parser')
# urls = soup.find_all('a')
#
# data = ''
#
# for url in urls:
#
#     if url.text !='':
#         data = data + url.text.replace('\n','') + '|'
# data = data[:-1]
# re.sub('[^0-9a-zA-Z\\s\\|]','',data)
# print(re.sub('[^0-9a-zA-Z\\s\\|\\.]','',data).lower())

str = urlparse("piggypiggy.blogspot.com")
print(str)
str2 = extract("http://news.piggypiggy.blogspot.com/hello/hyhy")
print(str2.registered_domain)
