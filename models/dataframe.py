import uuid

from config.db import Database
from lib.constants import BAMBOO_HASH, DATAFRAME_ID


def table():
    return Database.db().dataframes


def find(_hash):
    return table().find({BAMBOO_HASH: _hash})


def find_one(_hash):
    return table().find_one({BAMBOO_HASH: _hash})


def save(_hash):
    record = {BAMBOO_HASH: _hash, DATAFRAME_ID: uuid.uuid4().hex}
    table().insert(record)
    return record
