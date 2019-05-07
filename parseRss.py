import feedparser
from bs4 import BeautifulSoup
import urllib
import xmltodict

d = feedparser.parse('https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK=&type=13f-hr&company=&dateb=&owner=include&start=0&count=100&output=atom')

#print((d['entries']))

#for i in d['entries']:
#    print(i['title'])
#    print(i['link'])

url = d['entries'][0]['link']
page = urllib.request.urlopen(url).read()
soup = BeautifulSoup(page, 'html.parser')

for link in soup.find_all('a'):
    url = link.get('href')
    text = link.contents[0]
    if '.xml' in url and 'xml' in text and 'primary_doc' not in url:
        securities_list = url

# build proper url
securities_list_url = 'https://www.sec.gov{}'.format(securities_list)
data = urllib.request.urlopen(securities_list_url).read()


securities = xmltodict.parse(data)['informationTable']
print(securities)


#xmldoc = minidom.parse(doc)
#tree = ET.parse(doc)
#root = tree.getroot()

#for name, value in root.attrib.items():
#    print(name, value)
