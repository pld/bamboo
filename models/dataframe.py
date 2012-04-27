import uuid

from config.db import db
from lib.constants import BAMBOO_HASH, DATAFRAME_ID


TABLE = db().dataframes


def find(_hash):
    return TABLE.find({BAMBOO_HASH: _hash})


def find_one(_hash):
    return TABLE.find_one({BAMBOO_HASH: _hash})

def save(_hash):
    record = {BAMBOO_HASH: _hash, DATAFRAME_ID: uuid.uuid4().hex}
    TABLE.insert(record)
    return record
