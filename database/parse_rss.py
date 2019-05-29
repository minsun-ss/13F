import feedparser
from bs4 import BeautifulSoup
import urllib
import xmltodict
import pandas as pd
import re
import os
import datetime
import traceback


def build_company_list():
    '''
    Takes the RSS list of most recent 13F filings from SEC edgar and grabs the URL. Pages through for all the filings.
    There's usually some overlap between yesterday and today (and edits to old reports).
    :return: list of urls
    '''
    try:
        startnum = 0
        entryamt = 100
        url = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK=&type=13f-hr&company=&\dateb=&owner' \
              '=\include&start={}&count={}&output=atom'.format(startnum, entryamt)
        d = feedparser.parse(url)
        entrylist = d['entries']
        entries = d['entries']

        while len(entries) == 100:
            url = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK=&type=13f-hr&company=&dateb=&owner' \
                  '=include&start={}&count={}&output=atom'.format(startnum, entryamt)
            d = feedparser.parse(url)
            entries = d['entries']
            entrylist = entrylist + d['entries']
            startnum += 100
        return entrylist
    except Exception:
        traceback.print.exc()


def parse_link(entrylist, list_index):
    '''
    :param entrylist:
    :param list_index:
    :return: dataframe

    Takes a list of urls containing 13f filings, opens the xml filing with securities from each url, and collects them.
    Returns a pandas dataframe containing the securities holdings. Appends some other identifying information to the
    securities list (companyCIK, company, reporting and filing date).
    '''
    url = entrylist[list_index]['link']
    print('Working on {}, {}'.format(list_index, url))
    page = urllib.request.urlopen(url).read()
    soup = BeautifulSoup(page, 'html.parser')

    # get company name and CIK
    companyname = soup.find('span', class_='companyName').contents[0]
    companyname = re.sub(' \(Filer\)[\r\n]+', '', companyname)
    contents = soup.find('span', class_='companyName').contents[3]
    companyCIK = re.sub('<.*?>', '', str(contents))
    companyCIK = re.sub(' \(.*?\)', '', companyCIK)

    # find report date and filing date
    links = soup.find_all('div', class_='formGrouping')
    filingDate = links[0].find_all('div', class_='info')[0].contents[0]
    reportDate = links[1].find_all('div', class_='info')[0].contents[0]

    # find the securities
    for link in soup.find_all('a'):
        securities_url = link.get('href')
        text = link.contents[0]
        if '.xml' in securities_url.lower() and 'xml' in text.lower() and 'primary_doc' not in securities_url:
            securities_list = securities_url

    # build proper url
    securities_list_url = 'https://www.sec.gov{}'.format(securities_list)
    data = urllib.request.urlopen(securities_list_url).read()
    namespaces = {'http://www.sec.gov/edgar/document/thirteenf/informationtable': None}
    securities = xmltodict.parse(data, process_namespaces=True, namespaces=namespaces)
    securities_list = securities['informationTable']['infoTable']

    if isinstance(securities_list, list):
        df = pd.DataFrame.from_dict(securities_list)
    else:
        df = pd.DataFrame.from_dict(securities_list, orient='index').transpose()

    if 'putCall' not in df.columns:
        df.insert(5, 'putCall', '')
    if 'otherManager' not in df.columns:
        df.insert(7, 'otherManager', '')

    df['sshPrnamt'] = [d.get('sshPrnamt') for d in df.shrsOrPrnAmt]
    df['sshPrnamtType'] = [d.get('sshPrnamtType') for d in df.shrsOrPrnAmt]
    df['votingSole'] = [d.get('Sole') for d in df.votingAuthority]
    df['votingShared'] = [d.get('Shared') for d in df.votingAuthority]
    df['votingNone'] = [d.get('Shared') for d in df.votingAuthority]
    df['companyName'] = companyname
    df['companyCIK'] = companyCIK
    df['reportDate'] = reportDate
    df['filingDate'] = filingDate
    df.replace(r'\s+|\\n', ' ', regex=True, inplace=True)
    return df


def write_to_file(clist, filename):
    '''
    :param clist:
    :param filename:
    :return:

    Takes the list of securities and prints to csv (labeled filename). Includes a header at the very beginning. The
    company list is generated from build_company_list(). The csv is delimited by ^ as both commas and semi colons
    are common in text.
    '''
    for i in range(len(clist)):
        df = parse_link(clist, i)
        df.drop(columns=['shrsOrPrnAmt', 'votingAuthority'], inplace=True)

        if os.path.isfile(filename):
            pass # if it exists, it's assumed there's a header implemented already
        else:
            pd.DataFrame(df.columns).transpose().to_csv(filename, header=False, index=False, mode='a', sep='^')

        df.to_csv(filename, header=False, index=False, mode='a', sep='^')


def get_list_today():
    '''
    Pull the current days rss feed list and writes to csv
    :return:
    '''
    try:
        time_now = datetime.datetime.now().strftime('%Y%m%d')
        print(f'Getting list for {time_now}...')
        companylist = build_company_list()
        write_to_file(companylist, 'data/{}.csv'.format(time_now))
    except Exception:
        traceback.print.exc()