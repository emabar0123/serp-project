from enum import Enum

from redis import StrictRedis, RedisError


class RedisAdapter:
    def __init__(self, logger, config, db: int):
        self._logger = logger
        self._config = config
        self.host = config['host']
        self.port = config['port']
        self.password = config['password']
        self.username = config['username']
        self._db_number = db
        try:
            self.redis = StrictRedis(host=self.host, port=self.port, db=self._db_number, username=self.username,
                                     password=self.password)
        except Exception as e:
            self._logger.error(e)

    def increment(self, key):
        try:
            return self.redis.incr(key)
        except RedisError as e:
            self._logger.warning(f"Error occurred while incrementing counter: {e}")
            raise

    def decrement(self, key):
        try:
            return self.redis.decr(key)
        except RedisError as e:
            self._logger.warning(f"Error occurred while decrementing counter: {e}")
            raise

    def switch_db(self, new_db_index):
        try:
            temp_connection = StrictRedis(self.host, self.port, new_db_index)
            if temp_connection.ping():
                self._logger.info(f"Close connection to redis db and switch db to {new_db_index}")
                self.redis.close()
                self.redis = StrictRedis(self.host, self.port, new_db_index)
                temp_connection.close()
            return True
        except Exception as e:
            self._logger.error(e)
            return False

    def set(self, key, value):
        self.redis.set(key, value)


    '''
    add a key type list to db if exist append if not create new one 
    def set_to_list(self, key, value):
    :param
    key : string
    value : list
        
    '''
    def set_to_list(self, key, value):
        self.redis.rpush(key, *value)
    def get_from_hash(self, key, field):
        self.redis.hget(key, field)
    def set_to_hash(self, key, mapping):
        self.redis.hset(key, mapping=mapping)

    def get(self, key):
        self.redis.get(key)

    def delete(self, key):
        self.redis.delete(key)

    def is_connected(self) -> bool:
        try:
            self.redis.ping()
            return True
        except Exception as e:
            return False

    def flushall(self) -> bool:
        try:
            self.redis.flushall()
            return True
        except Exception as e:
            self._logger.error("Error occurred while flushing all data from Redis:", e)
            return False

    def key_exists(self, key):
        """
        Checks if a given key exists in Redis.

        :param key: The Redis key to check.
        """
        return self.redis.exists(key)

    def initialize_json_data(self, key, data):
        if not self.key_exists(key):
            self.redis.json().set(key, '$', data)

    def increment_counter(self, key, counter_name, increment=1) -> bool:
        """
        Increments a numeric counter within the JSON stored at a given key.

        :param key: The Redis key containing the JSON data.
        :param counter_name: The name of the counter within the JSON object to increment.
        :param increment: The amount by which to increment the counter (default is 1).
        :return bool
        """
        if self.key_exists(key):
            self.redis.hincrby(key, counter_name, increment)
            return True
        return False

    def update_status(self, key, status: Enum):
        """
        Updates the status field within the JSON stored at a given key.

        :param key: The Redis key containing the JSON data.
        :param status: The new status to set, must be an instance of the Status enum.
        """
        self.redis.json().set(key, '$.status', status.value)

    def get_json_data(self, key):
        """
          Retrieves the JSON data stored at a given key.

          :param key: The Redis key from which to retrieve the JSON data.
          :return: The JSON data stored at the key, or None if the key does not exist.
          """
        if not self.key_exists(key):
            return None
        return self.redis.json().get(key)


