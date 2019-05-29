from pymongo import MongoClient
from pymongo.write_concern import WriteConcern
import pandas as pd
from datetime import datetime, timedelta
import os
import traceback

DB_URI = os.environ['MONGO_DB_URI']
#mongo 'mongodb+srv://mongodb:04LdXyxTaB7s@cluster0-9nqth.mongodb.net/test'

def get_db():
    '''

    Configuration method to return db instance
    :return: returns db instance
    '''
    db = MongoClient(DB_URI, maxPoolSize=50, wtimeout=2500)['13f']
    return db

db = get_db()

def add_item(dataseries):
    '''
    Upserts a security line into the securities collection, with the following fields:

    - nameOfIssuer (name of the security issuer)
    - titleOfClass (type of security)
    - cusip
    - value (value, thousands, probably $?)
    - putCall (put or call)
    - investmentDiscretion
    - otherManager
    - sshPrnamt (shares or par amount)
    - sshPrnamtType (shares or par amount type)
    - votingSole
    - votingShared
    - votingNone
    - companyName (investment company name)
    - companyCIK (investment company ID)
    - reportDate (date of securities report/quarter end date)
    - filingDate (date of filing)

    If the security already exists for the particular CIK and report date, this will overwrite the current security
    figures; if it doesn't exist, it will insert.

    Returns mongodb's notification on successful/unsuccessful upsert
    :param dataseries:
    :return:
    '''

    security_match = {
        'nameOfIssuer': dataseries.nameOfIssuer,
        'cusip': dataseries.cusip,
        'companyCIK': dataseries.companyCIK,
        'putCall': dataseries.putCall,
        'sshPrnamt': dataseries.sshPrnamt,
        'reportDate': dataseries.reportDate
    }
    security_doc = {
        'nameOfIssuer': dataseries.nameOfIssuer,
        'titleOfClass': dataseries.titleOfClass,
        'cusip': dataseries.cusip,
        'value': dataseries.value,
        'putCall': dataseries.putCall,
        'investmentDiscretion': dataseries.investmentDiscretion,
        'otherManager': dataseries.otherManager,
        'sshPrnamt': dataseries.sshPrnamt,
        'sshPrnamtType': dataseries.sshPrnamtType,
        'votingSole': dataseries.votingSole,
        'votingShared': dataseries.votingShared,
        'votingNone': dataseries.votingNone,
        'companyName': dataseries.companyName,
        'companyCIK': dataseries.companyCIK,
        'reportDate': dataseries.reportDate,
        'filingDate': dataseries.filingDate
    }

    response = db.securities.update(
        security_match,
        {'$set': security_doc},
        upsert=True)

    # print out response message only if there is an overwrite
    if response['updatedExisting']:
        print(response)

    return response

def delete_securities():
    '''
    Deletes securities reports older than a certain number of days. Required in order to minimize free tier MongoDB
    limits.
    :return:
    '''

    # get today's date and subtract 7 days from it
    timecutoff = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

    # build pipeline
    security_match = {
        'filingDate': {'$lt': timecutoff}
    }

    response = db.securities.find_one(
        {security_match}
    )
    print(response)
    return response

def clean_data_folder():
    '''
    Purges the data folder of all .csv files for space issues
    '''
    directory = os.fsencode('data')
    for file in os.listdir(directory):
        file = os.fsdecode(file)
        filepath = f'data/{file}'
        try:
            os.remove(filepath)
            print(f'Removed {filepath}')
        except Exception:
            traceback.print.exc()

def add_all_csv():
    '''
    Adds all data files located in the /data folder to MongoDB cluster. Assumes all files in the folder to be
    propertly formatted .csv files.
    :return:
    '''
    directory = os.fsencode('data')
    for file in os.listdir(directory):
        file = os.fsdecode(file)
        filepath = f'data/{file}'
        print(f'Working on {filepath}...')
        df = pd.read_csv(filepath, sep='^')
        for row in df.itertuples():
            try:
                add_item(row)
            except Exception:
                traceback.print.exc()


delete_securities()
