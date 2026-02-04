import pymysql
from .abstract_database_adapter import DatabaseAdapter


class MySQLDatabaseAdapter(DatabaseAdapter):
    def __init__(self, logger, host, username, password, database, keep_connection=True):
        super().__init__(keep_connection, logger)
        self.logger = logger
        self.host = host
        self.username = username
        self.password = password
        self.database = database

    def connect(self):
        if not self.connection:
            self.connection = pymysql.connect(host=self.host, user=self.username, password=self.password,
                                              db=self.database)

    def disconnect(self):
        super().disconnect()

    def execute(self, command, args=None):
        return super().execute(command, args)
