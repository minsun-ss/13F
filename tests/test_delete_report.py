'''
Test file for database methods in db.py
'''

import database.db
import pytest
import os
from pymongo import MongoClient

DB_URI = os.environ['MONGO_DB_URI']

fake_report = {
    'companyCIK': '12345',
    'filingDate': '2019-01-01',
    'reportDate': '2019-01-01'
}

def get_db():
    '''
    Configuration method to return db instance
    :return: returns db instance
    '''
    db = MongoClient(DB_URI, maxPoolSize=50, wtimeout=2500)['13f']
    return db

db = get_db()

@pytest.mark.delete
def test_delete_report():
    # first add  comment to delete
    db.securities.insert_one(fake_report)
    response = database.db.delete_report('12345', '2019-01-01', '2019-01-01')
    assert (response == 1)

@pytest.mark.delete
def test_delete_old_data():
    # first add comment to delete
    db.securities.insert_one(fake_report)
    database.db.delete_old_securities()
    response = db.securities.find(fake_report)
    assert (response.count() == 0)