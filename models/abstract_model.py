from config.db import Database


class AbstractModel(object):

    @classmethod
    def set_table(cls, table_name):
        return Database.db()[table_name]
