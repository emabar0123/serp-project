from pho_db.adapters.mssql_database_adapter import MSSQLDatabaseAdapter
from pho_db.adapters.mysql_database_adapter import MySQLDatabaseAdapter
from pho_db.adapters.mongo_database_adapter import MongoDatabaseAdapter


class DatabaseAdapterFactory:
    @staticmethod
    def create_adapter(config, logger):
        db_type = config.get("database_type")
        keep_connection = config.get("keep_connection", True)

        if db_type == "mysql":
            return MySQLDatabaseAdapter(
                logger=logger,
                host=config.get("host"),
                username=config.get("username"),
                password=config.get("password"),
                database=config.get("database"),
                keep_connection=keep_connection
            )
        elif db_type == "mssql":
            return MSSQLDatabaseAdapter(
                logger=logger,
                host=config.get("host"),
                database=config.get("database"),
                username=config.get("username"),
                password=config.get("password"),
                driver=config.get("driver"),
                keep_connection=keep_connection
            )
        elif db_type == "mongodb":
            return MongoDatabaseAdapter(
                logger=logger,
                mongo_address=config.get("host"),
                mongo_port=config.get("port"),
                mongo_username=config.get("username"),
                mongo_password=config.get("password")
            )
        else:
            raise ValueError("Unsupported database type")