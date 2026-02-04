import abc
from typing import Dict


class IS3Client(abc.ABC):
    @abc.abstractmethod
    def init_s3(self, endpoint=None, access_key=None, secret_key=None, log_path=""):
        pass

    @abc.abstractmethod
    def list_all_objects(self, bucket_name=None, prefix=''):
        """
        List all the objects in a bucket
        :param prefix:limits the response to keys that begin with the specified prefix
        :param bucket_name: the name of the bucket containing the objects
        :return:List of all objects in the bucket
        """
        pass

    @abc.abstractmethod
    def list_files(self, bucket_name=None, prefix=''):
        """
        List only files from the bucket
        :param prefix: limits the response to keys that begin with the specified prefix
        :param bucket_name: the name of the bucket containing the objects
        :return:List only files in the bucket
        """
        pass

    @abc.abstractmethod
    def list_folders(self, bucket_name=None, prefix=''):
        """
        List only folders from the bucket
        :param prefix: limits the response to keys that begin with the specified prefix
        :param bucket_name: the name of the bucket containing the objects
        :return:List only folders in the bucket
        """
        pass

    @abc.abstractmethod
    def list_objects_by_path(self, path=None):
        """
        List only objects by path
        :param path: the path contains bucket name and prefix to return
        :return:List only objects by path
        """
        pass

    @abc.abstractmethod
    def download_file(self, bucket_name=None, key=None, directory=None, overwrite=None):
        """
        Download the contents of the s3 object and save to local file system
        :param bucket_name: the name of the bucket containing the object
        :param key: the name of the key to download
        :param directory: the local file path to store the download file
        :param overwrite: delete file in case the file exists
        :return: True in case the download success,False otherwise
        """
        pass

    @abc.abstractmethod
    def download_object(self, bucket_name=None, key=None, directory=None, append=False):
        """
        Download object from a specified bucket
        :param bucket_name: the name of the bucket containing the object
        :param key: the name of the key to download
        :param directory: the local file path to store the download file
        :param append:add new data to exists object
        :return: True in case the download success,False otherwise
        """
        pass

    @abc.abstractmethod
    def upload_file(self, bucket_name: str = None, file_path: str = None, key: str = None, metadata: Dict = None,
                    overwrite: bool = None):
        """
        Upload a file from a given path to specified bucket
        :param bucket_name: the name of the bucket containing the object
        :param file_path: the path to the file to uploaded
        :param key: the object key
        :param metadata: metadata of the object
        :param overwrite: delete file in case the file exists
        :return: True in case the upload was successful,False otherwise
        """
        pass

    @abc.abstractmethod
    # read about dictionris and list in function parameters
    def upload_object(self, bucket_name=None, key=None, data=None, metadata: Dict = None,
                      overwrite: bool = None):
        """
        Upload a file from a given path to specified bucket
        :param bucket_name: the name of the bucket containing the object
        :param key: the object key
        :param data: object data
        :param metadata: A map of metadata to store with the object in s3
        :param overwrite: delete file in case the file exists
        :return: True in case the upload was successful,False otherwise
        """
        pass

    @abc.abstractmethod
    def read_object(self, bucket_name=None, key=None):
        """
        Read object from a specified bucket, only for text files
        :param bucket_name: the name of the bucket containing the object
        :param key: key of the object
        :return: data of the object
        """
        pass

    @abc.abstractmethod
    def delete_object(self, bucket_name=None, key=None):
        """
        Remove object from the storage server
        :param bucket_name: the name of the bucket containing the object
        :param key: the key to remove
        :return:True in case the delete_object success, False otherwise
        """
        pass

    @abc.abstractmethod
    def get_object_metadata(self, bucket_name=None, key=None):
        """
        Get object from a specified bucket
        :param bucket_name: the name of the bucket containing the object
        :param key: the file name to return
        :return:metadata of object
        """
        pass

    @abc.abstractmethod
    def object_exists(self, bucket_name=None, key=None):
        """
        Exists object in a specified bucket
        :param bucket_name: the name of the bucket containing the object
        :param key: the file name to return
        :return:True in case the object exists, False otherwise
        """
        pass

