import uuid

from config.db import db
from lib.constants import BAMBOO_HASH, DATAFRAME_ID
from models import observation


TABLE = db().dataframes


def find(hash):
    return TABLE.find({BAMBOO_HASH: hash})


def find_one(hash):
    return TABLE.find_one({BAMBOO_HASH: hash})

def save(hash):
    e = {BAMBOO_HASH: hash, DATAFRAME_ID: uuid.uuid4().hex}
    TABLE.insert(e)
    return e
