from abc import ABC, abstractmethod


class DatabaseAdapter(ABC):
    '''
    DatabaseAdapter class is an abstract class for database connection adapters that
    can be used in phoenix.
    '''

    def __init__(self, keep_connection, logger):
        '''
        C'tor function
        :param keep_connection: Boolean, Sets whether to keep the connection up between executes or connect and
        disconnect before and after each execution and fetching
        '''
        self.logger = logger
        self.keep_connection = keep_connection
        self.connection = None

    @abstractmethod
    def connect(self):
        '''
        Connect method. Implemented differently for each database adapter
        :return:
        '''
        pass

    @abstractmethod
    def disconnect(self):
        '''
        Disconnect from database. currently common code for all adapters and is called by child class using the super()
        function.
        :return:
        '''
        if self.connection:
            self.connection.close()
            self.connection = None

    @abstractmethod
    def execute(self, command, args=None):
        '''
        Execute sql query. currently common code for all adapters and is called by child class using the super()
        function.
        :param command: string, SQL query
        :param args: tupple, SQL args in order to create db literals
        :return: tuple of results
        '''
        if not self.connection or not self.keep_connection:
            self.connect()
        with self.connection.cursor() as cursor:
            if args:
                cursor.execute(command, args)
            else:
                cursor.execute(command)
            result = cursor.fetchall()
        if not self.keep_connection:
            self.disconnect()
        return result

    def commit(self):
        '''
        commit the transaction rellevent only for relation database adapters
        function.
        :return: None
        '''
        if not self.connection or not self.keep_connection:
            return
        self.connection.commit()
        if not self.keep_connection:
            self.disconnect()
        return

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.keep_connection:
            self.disconnect()