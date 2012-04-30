from config.db import db
from lib.constants import DATAFRAME_ID, FORMULA

TABLE = db().calculations


def save(dframe, formula, **kwargs):
    record = {
        DATAFRAME_ID: dframe[DATAFRAME_ID],
        FORMULA: formula,
    }
    TABLE.insert(record)
