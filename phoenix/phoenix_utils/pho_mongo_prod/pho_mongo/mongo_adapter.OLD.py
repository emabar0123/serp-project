import sys
import traceback
import pymongo
from pymongo.errors import ConnectionFailure, OperationFailure, ServerSelectionTimeoutError, AutoReconnect
from pho_globals.structs.nosql_abs import NoSQLDatabase


class MongoAdapter(NoSQLDatabase):
    def __init__(self, mongo_address, mongo_port, mongo_username, mongo_password, logger):
        self._mongo_address = mongo_address
        self._mongo_port = mongo_port
        self._mongo_username = mongo_username
        self._mongo_password = mongo_password
        self._logger = logger
        self._logger = logger.getChild(self.__class__.__name__)
        self._client_connection = None

    def _init_mongo_client(self, server_timeout=4000):
        """
        connect to mongodb server
        :return: boolean
        server_timeout - control how long (in ms) the server will wait to find mongodb query
        """
        self._logger.info("Init Mongo adapter. Address: {0}:{1} USERNAME: {2}".format(self._mongo_address,
                                                                                      self._mongo_port,
                                                                                      self._mongo_username))

        try:
            self._client_connection = pymongo.MongoClient(host=self._mongo_address,
                                                          port=self._mongo_port,
                                                          username=self._mongo_username,
                                                          password=self._mongo_password,
                                                          serverSelectionTimeoutMS=server_timeout)
            return True
        except Exception as ex:
            self._logger.error("Failed to init MongoClient {0}:{1} - {2}".format(self._mongo_address,
                                                                                 self._mongo_port,
                                                                                 ex))
            self._logger.exception(ex)
            raise

    def _validate_connection(self):
        """
        send cheap command to mongodb server to check if connection is up.
        :return: True/False
        """
        # send query to the server to check if the connection is up
        try:
            # The "ismaster" is cheap command
            self._client_connection.admin.command('ismaster')
        except OperationFailure as ex:
            self._logger.error("mongodb connection Failed. check credential {0}".format(ex))
            self._logger.exception(ex)
            return False
        except ConnectionFailure as ex:
            self._logger.error("mongodb server is not available {0}".format(ex))
            self._logger.exception(ex)
            return False
        return True

    @property
    def connection_is_up(self):
        """
        Check the connection to Mongo server
        :return: True/False
        """
        return self._validate_connection()

    def is_collection(self, db_name, collection_name):
        """
        Check if collection exists in db
        :param db_name: mongo db name
        :param collection_name:
        :return: True / False
        """
        # check_collection
        if collection_name in self._client_connection[db_name].collection_names():
            return True
        return False

    def is_db(self, db_name):
        """
        check if db exists in mongo server
        :param db_name:
        :return: True/False
        """
        if db_name in self._client_connection.database_names():
            return True
        return False

    def get_documents(self, db_name, collection_name, filter):
        """
        Get documents by filter
        :param collection_name: Mongodb collection name
        :param db_name: Mongo DB name
        :param filter :type dictionary
        :return: iterators all match documents by filter
        """
        # if not self._is_collection(collection_name):
        #     raise ValueError("collection name not exists in db")

        try:
            return self._client_connection[db_name][collection_name].find(filter)

        except ServerSelectionTimeoutError as ex:
            raise SystemError("mongodb timeout error - {0}".format(ex))
        except AutoReconnect as ex:
            raise SystemError("mongodb is down {0}".format(ex))
        except Exception as ex:
            self._logger.error(ex)
            self._logger.exception(ex)
            raise Exception("Got unexpected error")

    def get_document(self, db_name, collection_name, filter):
        """
        Get documents by filter
        :param db_name: Mongodb DB name
        :param collection_name: Mongodb collection name
        :param filter :type dictionary
        :return: iterators all match documents by filter
        """

        try:
            return self._client_connection[db_name][collection_name].find_one(filter)

        except ServerSelectionTimeoutError as ex:
            raise SystemError("mongodb timeout error - {0}".format(ex))
        except AutoReconnect as ex:
            raise SystemError("mongodb is down {0}".format(ex))
        except Exception as ex:
            self._logger.exception(ex)

    def get_document_by_sort_value(self, db_name, collection_name, sort_field, high_direction=True, filter=None):
        """
        Get document by filter and sort field (Get document with the high/low value )
        :param db_name: Mongodb db name
        :param collection_name: Mongodb collection name
        :param filter :type dictionary
        :param sort_field: sorted field
        :param high_direction: True  - Get the document with high value
                               False - Get the document with low value
        """
        # sort direction
        if high_direction:
            sort_direction = pymongo.DESCENDING
        else:
            sort_direction = pymongo.ASCENDING

        try:
            return self._client_connection[db_name][collection_name].find_one(filter,
                                                                              sort=[(sort_field, sort_direction)])

        except ServerSelectionTimeoutError as ex:
            raise SystemError("mongodb timeout error - {0}".format(ex))
        except AutoReconnect as ex:
            raise SystemError("mongodb is down {0}".format(ex))
        except Exception as ex:
            self._logger.exception(ex)
            raise Exception("Failed to get document - Got unexpected error")

    def insert_document(self, db_name, collection_name, data):
        """
        insert document to mongodb server
        :param db_name: mongodb db name
        :param collection_name: mongodb collection name
        :param data: data to insert :type dict
        :return: True/False
        """
        # check data type
        if not isinstance(data, dict):
            self._logger.error("data type must be a dictionary")
            return False
        try:
            # insert document to mongodb server
            mongo_document_id = self._client_connection[db_name][collection_name].insert_one(data)
            if not mongo_document_id:
                self._logger.error("Failed to insert document")
                return False
            self._logger.info(
                "Added document successfully. Mongo document id: {0}".format(mongo_document_id.inserted_id))
            return True

        except ServerSelectionTimeoutError as ex:
            self._logger.error("mongodb timeout error")
            self._logger.exception(ex)
            return False
        except AutoReconnect as ex:
            self._logger.error("mongodb is down")
            self._logger.exception(ex)
            return False
        except Exception as ex:
            self._logger.error("Failed to get document - Got unexpected error")
            self._logger.exception(ex)
            return False

    def increase_document_value(self, db_name, collection_name, filter, increase_field, increase_count):
        """
        Finds a single document and updates it, returning the increased updated field value.
        :param db_name: Mongodb db name
        :param collection_name: Mongodb collection name
        :param filter: A query that matches the document to update :type: dictionary
        :param increase_field: increase field
        :param increase_count: increase count value :type: integer
        :return:  The increase_field value (after update)
        """
        #
        if not isinstance(filter, dict):
            self._logger.error("filter must be a dictionary")
            return None

        try:
            # Finds a single document and updates it, returning either the updated document.
            # Use $inc update operations to increase field by count
            document = self._client_connection[db_name][collection_name].find_one_and_update(filter=filter,
                                                                                             update={
                                                                                                 "$inc": {
                                                                                                     increase_field:
                                                                                                         increase_count
                                                                                                 }
                                                                                             },
                                                                                             return_documnet=pymongo.ReturnDocument.AFTER)

            if not document:
                self._logger.error("update document failed or document not found")
                return None

            # Return the increase_field value
            return document[increase_field]

        except ServerSelectionTimeoutError as ex:
            self._logger.error("mongodb timeout error")
            self._logger.exception(ex)
            return None
        except AutoReconnect as ex:
            self._logger.error("mongodb is down")
            self._logger.exception(ex)
            return None
        except Exception as ex:
            self._logger.debug(traceback.format_exc(sys.exc_info()))
            self._logger.error("Failed to get document - Got unexpected error")
            self._logger.exception(ex)

            return None

    def update_document(self, db_name, collection_name, filter, update):
        """
        Update a single document matching the filter
        Use $set update operation to update value, if key exists overwrite it.
        :param db_name: Mongodb db name
        :param collection_name: mongodb collection name
        :param filter: A query that matches the document to update :type: dictionary
        :param update: The value to update :type: dictionary  example: {KEY: UPDATE_VALUE}
        :return: True/False
        """

        if not isinstance(filter, dict):
            self._logger.error("filter must be a dictionary")
            return None

        try:
            if not self._client_connection[db_name][collection_name].update_one(filter=filter, update={"$set": update}):
                self._logger.error("Failed to update document {0}".format(collection_name))
                return False
            return True
        except ServerSelectionTimeoutError as ex:
            self._logger.error("mongodb timeout error")
            self._logger.exception(ex)
            return False
        except AutoReconnect as ex:
            self._logger.error("mongodb is down")
            self._logger.exception(ex)

            return False
        except Exception as ex:
            self._logger.error("Failed to get document - Got unexpected error {0}".format(ex))
            self._logger.exception(ex)
            return False

    def delete_document(self, db_name, collection_name, filter):
        """
        Delete a single document matching the filter.
        :param db_name: Mongodb db name
        :param collection_name: mongodb collection name
        :param filter: A query that matches the document to update :type: dictionary
        :return: True/False
        """
        """
        insert document to mongodb server
        :param db_name: mongodb db name
        :param collection_name: mongodb collection name
        :param data: data to insert :type dict
        :return: True/False
        """
        try:
            # delete document to mongodb server
            result = self._client_connection[db_name][collection_name].delete_one(filter)
            if result.deleted_count == 0:
                self._logger.error("Failed to delete document")
                return False
            self._logger.debug(f"Number of documents deleted: {result.deleted_count}")
            self._logger.info("Delete document successfully")
            return True

        except ServerSelectionTimeoutError as ex:
            self._logger.error("mongodb timeout error")
            self._logger.exception(ex)
            return False
        except AutoReconnect as ex:
            self._logger.error("mongodb is down")
            self._logger.exception(ex)

            return False
        except Exception as ex:
            self._logger.error("Failed to get document - Got unexpected error {0}".format(ex))
            self._logger.exception(ex)
            return False
