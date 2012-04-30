from config.db import db
from lib.constants import DATAFRAME_ID, FORMULA, NAME

TABLE = db().calculations


def save(dframe, formula, name, **kwargs):
    record = {
        DATAFRAME_ID: dframe[DATAFRAME_ID],
        FORMULA: formula,
        NAME: name,
    }
    TABLE.insert(record)

def find(dframe):
    return [x for x in TABLE.find({
        DATAFRAME_ID: dframe[DATAFRAME_ID],
    }]
