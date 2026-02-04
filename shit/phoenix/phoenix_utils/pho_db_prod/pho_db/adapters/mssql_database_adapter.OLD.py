import pyodbc
from .abstract_database_adapter import DatabaseAdapter


class MSSQLDatabaseAdapter(DatabaseAdapter):
    def __init__(self, logger, host, database, username, password, driver, keep_connection=True):
        super().__init__(keep_connection, logger)
        self.logger = logger
        self.host = host
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver

    def connect(self):
        if not self.connection:
            self.connection = pyodbc.connect(
                f'DRIVER={self.driver};SERVER={self.host};DATABASE={self.database};UID={self.username};PWD={self.password}'
            )

    def disconnect(self):
        super().disconnect()

    def execute(self, command, args=None):
        return super().execute(command, args)