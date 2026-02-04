import os
import boto3
import botocore.exceptions
from typing import Dict


class AmazonS3Util:
    _boto3_client = None

    @staticmethod
    def init_s3(endpoint=None, access_key=None, secret_key=None):
        AmazonS3Util._boto3_client = boto3.client('s3',
                                                  endpoint_url=endpoint,
                                                  aws_access_key_id=access_key,
                                                  aws_secret_access_key=secret_key)

    @staticmethod
    def bucket_exists(bucket_name=None):
        """
        Check if the bucket exists
        :param bucket_name: the bucket name
        :return:True in case the bucket exists, false if not, raise otherwise
        """
        if not bucket_name:
            raise Exception("bucket_name invalid")
        try:
            AmazonS3Util._boto3_client.head_bucket(Bucket=bucket_name)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            raise
        return True

    @staticmethod
    def object_exists(bucket_name: object = None, key_name: object = None) -> object:
        """
        Check object exist in bucket,
        :param bucket_name: the name of the bucket containing the object
        :param key_name: the key name
        :return:True in case the object exists, False otherwise
        """
        try:
            if not all((bucket_name, key_name)):
                raise Exception(f"One or more arguments is invalid: bucket_name={bucket_name},  key_name={key_name}")
            AmazonS3Util._boto3_client.head_object(Bucket=bucket_name, Key=key_name)

        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            raise
        return True

    @staticmethod
    def list_objects_from_bucket(bucket_name=None, max_keys=None, prefix=''):
        """
        List all the objects in a bucket
            :param prefix:limits the response to keys that begin with the specified prefix
            :param max_keys:the maximum number of keys returned to the response
            :param bucket_name: the bucket name of the bucket containing the objects
            :return:A list of all objects in the bucket
        """
        if not all((bucket_name, max_keys)):
            raise Exception(f"One or more arguments is invalid: bucket_name={bucket_name},  max_keys={max_keys}")
        continuation_token = ''
        objects = []
        response, objects = AmazonS3Util.list_objects(bucket_name, max_keys, prefix, continuation_token, objects)

        while response['IsTruncated']:
            continuation_token = response['NextContinuationToken']
            response, objects = AmazonS3Util.list_objects(bucket_name, max_keys, prefix, continuation_token, objects)
        return objects

    @staticmethod
    def download_file(bucket_name=None, key=None, directory=None):
        """
        Download the contents of the s3 object and save to local file system
        :param bucket_name: the name of the bucket containing the object
        :param key: the name of the object to download
        :param directory: the local file path to store the download file
        :return: True in case the download success, raise in case of exception
        """
        if not all((bucket_name, key, directory)):
            raise Exception(
                f"One or more arguments is invalid: bucket_name={bucket_name},  max_keys={key}, directory={directory}")
        destination_file = os.path.join(directory, key)
        AmazonS3Util._boto3_client.download_file(bucket_name, key, destination_file)
        return True

    @staticmethod
    def download_object(bucket_name=None, key=None, directory=None, append=None):
        """
        Download the contents of the s3 object and writes it to a file-like object in memory
        :param bucket_name: the name of the bucket containing the object
        :param key: the name of the key to download
        :param directory: the local file path to store the download file
        :param append: add data to file or overwrite
        :return: True in case the download was successful, raise in case of exception
        """
        if not all((bucket_name, key, directory)):
            raise Exception(
                f"One or more arguments is invalid: bucket_name={bucket_name},  max_keys={key}, directory={directory}")
        destination_file = os.path.join(directory, key)
        open_flag = "wb"
        if append:
            open_flag = "ab"

        with open(destination_file, open_flag) as file:
            AmazonS3Util._boto3_client.download_fileobj(bucket_name, key, file)
        return True

    @staticmethod
    def upload_file(bucket_name: str = None, file_path: str = None, key: str = None, metadata: Dict = None):
        """
        Upload a file from a given path to specified bucket
        :param bucket_name: the name of the bucket containing the object
        :param file_path: the file path to uploaded
        :param key:the object key
        :param metadata:metadata of the object
        :return: None, raise in case of exception
        """
        if not all((bucket_name, file_path, key)):
            raise Exception(
                f"One or more arguments is invalid: bucket_name={bucket_name}, file_path={file_path}, key={key}")

        AmazonS3Util._boto3_client.upload_file(file_path,
                                               Bucket=bucket_name,
                                               Key=key,
                                               ExtraArgs=metadata)

    @staticmethod
    def upload_object(bucket_name=None, key=None, data=None, metadata: Dict = None):
        """
        Add an object to a bucket
        :param bucket_name: the name of the bucket containing the object
        :param key: the object key
        :param data: object data
        :param metadata: a map of metadata to store with the object in s3
        :return: None,raise in case of exception
        """
        if not all((bucket_name, key, data)):
            raise Exception(f"One or more arguments is invalid: bucket_name={bucket_name}, key={key}, data={data}")
        if metadata:
            AmazonS3Util._boto3_client.put_object(Body=data,
                                                  Bucket=bucket_name,
                                                  Key=key,
                                                  Metadata=metadata)
        else:
            AmazonS3Util._boto3_client.put_object(Body=data,
                                                  Bucket=bucket_name,
                                                  Key=key)

    @staticmethod
    def read_object(bucket_name=None, key=None):
        """
        Read object from a specified bucket, only for text files
        :param bucket_name: the name of the bucket containing the object
        :param key: key of the object
        :return: data of the object
        """
        if not all((bucket_name, key)):
            raise Exception(f"One or more arguments is invalid: bucket_name={bucket_name}, key={key}")
        s3_object = AmazonS3Util._boto3_client.get_object(Bucket=bucket_name, Key=key)
        body = s3_object.get('Body')
        if not body:
            raise Exception(f"unable to read the file {key} ")
        return body.read().decode('utf-8').strip()

    @staticmethod
    def delete_object(bucket_name=None, key=None):
        """
      Remove a file from the storage server
       :param bucket_name: the name of the bucket containing the object
       :param key: key name of the object to delete
       :return:None,  raise in case of exception
       """
        if not all((bucket_name, key)):
            raise Exception(f"One or more arguments is invalid: bucket_name={bucket_name}, key={key}")
        AmazonS3Util._boto3_client.delete_object(Bucket=bucket_name, Key=key)

    @staticmethod
    def get_object_metadata(bucket_name: object = None, key: object = None) -> object:
        """
        Metadata for an object
        :param bucket_name: the name of the bucket containing the object
        :param key: the object key
        :return:metadata of the object
        """
        if not all((bucket_name, key)):
            raise Exception(f"One or more arguments is invalid: bucket_name={bucket_name}, key={key}")
        return AmazonS3Util._boto3_client.head_object(Bucket=bucket_name, Key=key)

    @staticmethod
    def create_bucket(bucket_name=None):
        """
        Create_bucket
        :param bucket_name:the bucket name
        :return:True if bucket exists, False if not
        """
        if not bucket_name:
            raise Exception("bucket_name invalid")
        AmazonS3Util._boto3_client.create_bucket(bucket_name)
        return AmazonS3Util.bucket_exists(bucket_name)

    @staticmethod
    def list_objects(bucket_name=None, max_keys=None, prefix='', continuation_token='', objects=[]):
        response = AmazonS3Util._boto3_client.list_objects_v2(
            Bucket=bucket_name,
            ContinuationToken=continuation_token,
            MaxKeys=max_keys,
            Prefix=prefix)

        if 'Contents' in response:
            for obj in response['Contents']:
                objects.append(obj)

        return response, objects
