from logging import Logger
from time import sleep
import os
import shutil
import time
import csv
import hashlib

from pho_globals.pho_exceptions import ValidationError


def get_unique_file_name(file_name, extension):
    return "{0}_{1}.{2}".format(file_name, str(time.time()).split(".")[0], extension)

def get_directory_size(path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)

            # Skip if it is a symbolic link
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)
    return total_size


def wait_for_copy_completion(logger, directory, check_interval=1, stable_count=3):
    stable_size = get_directory_size(directory)
    stable_checks = 0

    while stable_checks < stable_count:
        sleep(check_interval)
        current_size = get_directory_size(directory)
        if current_size == stable_size:
            stable_checks += 1
        else:
            return False

        logger.info(f"Current size: {current_size}, Stable checks {stable_checks}")
    return True


def check_if_file_copy_complete(filename):
    initial_size = get_file_size(filename)
    sleep(0.2)
    final_size = get_file_size(filename)

    if initial_size == final_size and initial_size != False:
        return True
    else:
        return False


def get_file_size(filename):
    try:
        with open(filename, "r") as file:
            file.seek(0, 2)
            size = file.tell()
            return size

    except Exception as e:
        return False


def calculate_md5_of_file(file_path: str, chunk_size=1073741824) -> str:
    """
    Calculate the MD5 hash of a file.
    default value of chunk_size is 1GB
    """
    md5_hash = hashlib.md5()

    with open(file_path, 'rb') as file:
        chunk = file.read(chunk_size)
        while len(chunk) > 0:
            md5_hash.update(chunk)
            chunk = file.read(chunk_size)

    return md5_hash.hexdigest()


def validate_file_operation(src_file_path: str, dst_dir_path: str, logger) -> bool:
    """
    Check if file operation from src_file_path to dst_dir_path was succeeded.
    Compare file sizes and MD5 hash of src and dst file.
    :return: True if the operation was succeeded, False otherwise.
    """
    file_name = os.path.basename(src_file_path)
    dst_file_path = os.path.join(dst_dir_path, file_name)

    # File exists, check sizes and hashes
    src_file_size = os.path.getsize(src_file_path)
    dst_file_size = os.path.getsize(dst_file_path)

    if src_file_size != dst_file_size:
        logger.error(f"File size mismatch: {src_file_size} != {dst_file_size}")
        return False

    src_hash = calculate_md5_of_file(src_file_path)
    dst_hash = calculate_md5_of_file(dst_file_path)

    # If sizes are different or hashes differ, delete the destination file and return False
    if src_hash != dst_hash:
        logger.error(f"File hash mismatch: {src_hash} != {dst_hash}")
        return False

    return True


def move_or_copy_file_with_validation(src_file_path: str, dst_dir_path: str, logger: Logger,
                              keep_src: bool = False):
    """
    Copy file and validate file operation using validate_file_operation.
    :param src_file_path: Source file path
    :param dst_dir_path: Destination directory path
    :param logger: Logger object
    :param keep_src: True to keep the src file and copy it, False otherwise, so the operation is move(copy and delete).
    :return: True if the file succeeded the file operation after validation, False otherwise.
    """
    file_name = os.path.basename(src_file_path)
    dst_file_path = os.path.join(dst_dir_path, file_name)

    if not os.path.exists(src_file_path):
        logger.error(f"Source file does not exist: {src_file_path}")
        raise FileNotFoundError

    if os.path.getsize(src_file_path) == 0:
        logger.warning(f"Source file empty, deleting {src_file_path}")
        os.remove(src_file_path)
        raise ValueError

    if not os.path.exists(dst_file_path):
        shutil.copy(src_file_path, dst_file_path)

    if validate_file_operation(src_file_path, dst_dir_path, logger):
        if not keep_src:
            os.remove(src_file_path)
    else:
        os.remove(dst_file_path)
        raise ValidationError


def move_or_copy_file_with_retries(src_file_path: str, dst_dir_path: str, retries: int, delay: int, logger: Logger,
                           keep_src: bool = False):
    return retry_file_operation(src_file_path, dst_dir_path, retries, delay, move_or_copy_file_with_validation, logger,
                                keep_src)


def retry_file_operation(src_dir_path: str, dst_dir_path: str, retries: int, delay: int, operation,
                         logger: Logger, keep_src: bool = False) -> bool:
    """
    Move or copy a file(based on the given operation value) from src dir to dst dir with a given number of retries
    Args:
        src_dir_path: Source directory path of the file
        dst_dir_path: Destination directory path of the file
        retries: Number of retries on moving file if -1 retry forever
        delay: Number of seconds to wait between moving file
        logger: logger object
        operation: shutil.copy or shutil.move

    Returns:
    1. True - if file was successfully moved or copy and a message
    2. False - if file was not successfully moved or copied return its specific error message
    """
    while True:
        if retries > 0:
            retries -= 1
            log_retry = retries
        else:
            log_retry = "infinity"
        try:
            operation(src_dir_path, dst_dir_path, logger, keep_src)
            logger.info(f"Succeeded using {operation} from {src_dir_path} to {dst_dir_path}")
            return True
        except ValidationError as e:
            logger.error(f"Validation file failed (Size or hash mismatched), error:{e}")
        except ValueError as e:
            logger.error(f"Source file is empty {src_dir_path}, error: {e}")
            return False
        except PermissionError as e:
            logger.error(
                f"Permission error attempt remaining {log_retry}: {e}. Failed using {operation} file {src_dir_path} to {dst_dir_path}, Retrying in {delay} seconds")
        except FileNotFoundError as e:
            logger.error(f"File not found: {e}.")
            return False
        except Exception as e:
            logger.error(
                f"Unexpected error attempt remaining {log_retry}: {e}. Failed using {operation} file {src_dir_path} to {dst_dir_path}, Retrying in {delay} seconds")

        # Wait before retrying
        time.sleep(delay)

        # If all retries failed
        return False


class csv_read():
    def __init__(self):
        self.f = None

    def __del__(self):
        if self.f:
            if not self.f.closed:
                self.f.close()

    def close(self):
        if self.f:
            if not self.f.closed:
                self.f.close()
            if self.f:
                self.f = None

    def read_from_csv(self, file_path, include_headers=True, encoding="utf-16le", delimiter='"', lines_count=0):
        if self.f:
            if not self.f.closed:
                self.f.close()
        self.f = open(file_path, 'r', encoding=encoding, errors="ignore")
        reader = csv.reader(self.f, delimiter=delimiter)
        if include_headers:
            for x in range(lines_count):
                headers = next(reader)
                lines = reader
        else:
            lines = reader
        return headers, lines
