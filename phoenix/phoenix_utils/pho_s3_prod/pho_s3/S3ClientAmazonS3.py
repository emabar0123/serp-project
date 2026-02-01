import os
from abc import ABC
from typing import Dict
from .Abstract.IS3ClientAmazonS3 import IS3Client
from .AmazonS3Utility import AmazonS3Util
from .logging_utils import get_logger
from .S3objct import S3Objct
from enum import Enum


class ListObject(Enum):
    all_objects = 1
    only_files = 2
    only_folders = 3


class S3ClientAmazon(IS3Client, ABC):

    def __init__(self):
        self.max_keys = None
        self.log = None
        self.create_bucket_if_not_exists = None

    def init_s3(self, endpoint=None, access_key=None, secret_key=None, create_bucket_if_not_exists=False,
                log_path="", max_keys=None) -> object:
        if not all((endpoint, access_key, secret_key, log_path, max_keys)):
            raise Exception(
                f"One or more arguments is invalid: endpoint={endpoint}, access_key={access_key}, access_key={access_key}, log_path={log_path}, max_keys={max_keys}")
        try:
            AmazonS3Util.init_s3(endpoint, access_key, secret_key)
            self.create_bucket_if_not_exists = create_bucket_if_not_exists
            self.max_keys = max_keys
            self.log = get_logger(__class__.__name__, os.path.join(log_path))
        except Exception:
            raise

    def list_all_objects(self, bucket_name=None, prefix=''):
        """
        List all the objects in a bucket
        :param prefix:limits the response to keys that begin with the specified prefix
        :param bucket_name: the name of the bucket containing the objects
        :return:List of all objects in the bucket
        """
        try:
            return self.__list_objects(bucket_name, prefix, ListObject.all_objects)
        except Exception as e:
            self.log.error(e)
            raise

    def list_objects_by_path(self, path=None):
        """
        List only objects by path
        :param path: the path contains bucket name and prefix to return
        :return:List only objects by path
        """
        if path.startswith("/"):
            path = path.split("/", 1)[1]
        bucket_name, prefix = path.split("/", 1)
        try:
            return self.__list_objects(bucket_name, prefix, ListObject.all_objects)
        except Exception as e:
            self.log.error(e)
            raise

    def list_files(self, bucket_name=None, prefix=''):
        """
        List only files from the bucket
        :param prefix: limits the response to keys that begin with the specified prefix
        :param bucket_name: the name of the bucket containing the objects
        :return:List only files in the bucket
        """
        try:
            return self.__list_objects(bucket_name, prefix, ListObject.only_files)
        except Exception as e:
            self.log.error(e)
            raise

    def list_folders(self, bucket_name=None, prefix=''):
        """
        List only folders from the bucket
        :param prefix: limits the response to keys that begin with the specified prefix
        :param bucket_name: the name of the bucket containing the objects
        :return:List only folders in the bucket
        """
        try:
            return self.__list_objects(bucket_name, prefix, ListObject.only_folders)
        except Exception as e:
            self.log.error(e)
            raise

    def download_file(self, bucket_name=None, key=None, directory=None, overwrite=None):
        """
        Download the contents of the s3 object and save to local file system
        :param bucket_name: the name of the bucket containing the object
        :param key: the name of the key to download
        :param directory: the local file path to store the download file
        :param overwrite: delete file in case the file exists
        :return: True in case the download success,False otherwise
        """
        if not all((bucket_name, key, directory)):
            raise Exception(
                f"One or more arguments is invalid: bucket_name={bucket_name}, key={key}, directory={directory}")
        try:
            if not AmazonS3Util.bucket_exists(bucket_name):
                raise Exception(f"Bucket: {bucket_name} not exists")
            if not AmazonS3Util.object_exists(bucket_name, key):
                raise Exception(f"Object: {key} not exists in bucket: {bucket_name} ")
            if os.path.exists(os.path.join(directory, key)) and not overwrite:
                raise Exception(f"{key} exists in directory: {directory}")
            return AmazonS3Util.download_file(bucket_name, key, directory)
        except Exception as e:
            self.log.error(e)
            raise

    def download_object(self, bucket_name=None, key=None, directory=None, append=False):
        """
        Download object from a specified bucket
        :param bucket_name: the name of the bucket containing the object
        :param key: the name of the key to download
        :param directory: the local file path to store the download file
        :param append:add new data to exists object
        :return: True in case the download success,False otherwise
        """
        if not all((bucket_name, key, directory)):
            raise Exception(
                f"One or more arguments is invalid: bucket_name={bucket_name}, key={key}, directory={directory}")
        try:
            if not AmazonS3Util.bucket_exists(bucket_name):
                raise Exception(f"Bucket: {bucket_name} not exists")
            if not AmazonS3Util.object_exists(bucket_name, key):
                raise Exception(f"Object: {key} not exists in bucket: {bucket_name} ")
            return AmazonS3Util.download_object(bucket_name, key, directory, append)
        except Exception as e:
            self.log.error(e)
            raise

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
        if not all((bucket_name, file_path, key)):
            raise Exception(
                f"One or more arguments is invalid: bucket_name={bucket_name}, file_path={file_path}, key={key}")
        try:
            if not AmazonS3Util.bucket_exists(bucket_name):
                if self.create_bucket_if_not_exists and AmazonS3Util.create_bucket(bucket_name):
                    pass
                else:
                    raise Exception(f"Bucket: {bucket_name} not exists")
            if not os.path.exists(file_path):
                raise Exception(f"{file_path} Not Found")
            if AmazonS3Util.object_exists(bucket_name, key) and not overwrite:
                key = self.__create_new_name_for_key(bucket_name, key)
            AmazonS3Util.upload_file(bucket_name, file_path, key, metadata)
            if AmazonS3Util.object_exists(bucket_name, key):
                return True
            return False
        except Exception as e:
            self.log.error(e)
            raise

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
        if not all((bucket_name, key, data)):
            raise Exception(
                f"One or more arguments is invalid: bucket_name={bucket_name}, key={key}, data={data}")
        try:
            if not AmazonS3Util.bucket_exists(bucket_name):
                if self.create_bucket_if_not_exists and AmazonS3Util.create_bucket(bucket_name):
                    pass
                else:
                    raise Exception(f"Bucket: {bucket_name} not exists")
            if AmazonS3Util.object_exists(bucket_name, key) and not overwrite:
                key = self.__create_new_name_for_key(bucket_name, key)
            AmazonS3Util.upload_object(bucket_name, key, data, metadata)
            if not AmazonS3Util.object_exists(bucket_name, key):
                return False
            return True
        except Exception as e:
            self.log.error(e)
            raise

    def read_object(self, bucket_name=None, key=None):
        """
        Read object from a specified bucket, only for text files
        :param bucket_name: the name of the bucket containing the object
        :param key: key of the object
        :return: data of the object
        """
        if not all((bucket_name, key)):
            raise Exception(f"One or more arguments is invalid: bucket_name={bucket_name}, key={key}")
        try:
            if not AmazonS3Util.bucket_exists(bucket_name):
                raise Exception(f"Bucket: {bucket_name} not exists")
            if key[-1] == '/':
                raise Exception(f"{key}  - this object is not a file")
            if not AmazonS3Util.object_exists(bucket_name, key):
                raise Exception(f"{key} not exists in bucket {bucket_name}")
            else:
                return AmazonS3Util.read_object(bucket_name, key)
        except Exception as e:
            self.log.error(e)
            raise

    def delete_object(self, bucket_name=None, key=None):
        """
        Remove object from the storage server
        :param bucket_name: the name of the bucket containing the object
        :param key: the key to remove
        :return:True in case the delete_object success, False otherwise
        """
        if not all((bucket_name, key)):
            raise Exception(f"One or more arguments is invalid: bucket_name={bucket_name}, key={key}")
        try:
            if not AmazonS3Util.bucket_exists(bucket_name):
                raise Exception(f"Bucket: {bucket_name} not exists")
            if not AmazonS3Util.object_exists(bucket_name, key):
                raise Exception(f"{key} not exists in bucket {bucket_name}")
            AmazonS3Util.delete_object(bucket_name, key)
            if not AmazonS3Util.object_exists(bucket_name, key):
                return False
            return False

        except Exception as e:
            self.log.error(e)
            raise

    def get_object_metadata(self, bucket_name=None, key=None):
        """
        Get object from a specified bucket
        :param bucket_name: the name of the bucket containing the object
        :param key: the file name to return
        :return:metadata of object
        """
        if not all((bucket_name, key)):
            raise Exception(f"One or more arguments is invalid: bucket_name={bucket_name}, key={key}")
        try:
            if AmazonS3Util.bucket_exists(bucket_name):
                return AmazonS3Util.get_object_metadata(bucket_name, key)
            else:
                raise Exception(f"Bucket: {bucket_name} not exists")
        except Exception as e:
            self.log.error(e)
            raise

    def object_exists(self, bucket_name=None, key=None):
        """
            Exists object in a specified bucket
            :param bucket_name: the name of the bucket containing the object
            :param key: the file name to return
            :return:True in case the object exists, False otherwise
            """
        if not all((bucket_name, key)):
            raise Exception(f"One or more arguments is invalid: bucket_name={bucket_name}, key={key}")
        try:
            if not AmazonS3Util.bucket_exists(bucket_name):
                raise Exception(f"Bucket: {bucket_name} not exists")
            if not AmazonS3Util.object_exists(bucket_name, key):
                return False
            return True
        except Exception as e:
            self.log.error(e)
            raise

    def __list_objects(self, bucket_name=None, prefix='', type_objects=1):
        """
        List objects from the bucket
        :param bucket_name: the name of the bucket containing the object
        :param prefix: limits the response to keys that begin with the specified prefix
        :param type_objects: 1 for all objects,2 for files,3 for folders
        :return: List objects from the bucket
        """
        if not bucket_name:
            raise Exception("bucket_name argument is invalid")
        if not AmazonS3Util.bucket_exists(bucket_name):
            raise Exception(f"Bucket: {bucket_name} not exists")
        list_obj = AmazonS3Util.list_objects_from_bucket(bucket_name, self.max_keys, prefix)
        if len(list_obj) == 0:
            raise Exception(f"Not found objects in bucket : {bucket_name}")
        for obj in list_obj:
            if obj['Key'][-1] == '/' and \
                    (type_objects == ListObject.all_objects or type_objects == ListObject.only_folders):
                s3object = S3Objct(obj['Key'], obj['Size'], obj['LastModified'], 'Folder')
                yield s3object
            if obj['Key'][-1] != '/' and \
                    (type_objects == ListObject.all_objects or type_objects == ListObject.only_files):
                s3object = S3Objct(obj['Key'], obj['Size'], obj['LastModified'], 'File')
                yield s3object

    def __create_new_name_for_key(self, bucket_name=None, key=None):
        """
        Create new name for key exists
        :param bucket_name: the name of the bucket containing the key
        :param key:the key exists
        :return:new name : key exists + number not exists
        """
        i: int = 0
        file_exists: bool = True
        if "." not in key:
            name = key
        else:
            name, ending = key.rsplit('.', 1)
        while file_exists:
            i = i + 1
            if "." in key:
                key = name + str(i) + '.' + ending
            else:
                key = name + str(i)
            file_exists = AmazonS3Util.object_exists(bucket_name, key)
        return key
