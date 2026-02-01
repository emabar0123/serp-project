import hashlib
import os
import time
import traceback

def calculate_hashes(file_path: str) -> dict:
    """
    Calculate SHA-256, SHA-1, and MD5 hashes of a file.

    Args:
        file_path (str): The path to the file.

    Returns:
        dict: A dictionary containing the hash values for each algorithm.
    """
    try:
        hash_values = {"sha256": hashlib.sha256(), "sha1": hashlib.sha1(), "md5": hashlib.md5()}

        with open(file_path, "rb") as file:
            while True:
                data = file.read(65536)  # 64 KB at a time
                if not data:
                    break
                for hash_algorithm in hash_values.values():
                    hash_algorithm.update(data)

        hash_results = {algorithm: hash_obj.hexdigest() for algorithm, hash_obj in hash_values.items()}
        return hash_results

    except FileNotFoundError:
        raise FileNotFoundError(f"The file '{file_path}' was not found.")
    except Exception as e:
        raise Exception(f"An error occurred: {str(e)}")


def sha256_string(input_string):
    sha256_hash = hashlib.sha256()
    sha256_hash.update(input_string.encode('utf-8'))
    return sha256_hash.hexdigest()


def convert_windows_path_to_linux(windows_path):
    linux_path = windows_path.replace('\\', '/')
    linux_path = os.path.normpath(linux_path)
    return linux_path




def get_directory_size(directory_path):
    total_size = 0
    for dirpath, dirnames, filesnames in os.walk(directory_path):
        for filename in filesnames:
            file_path = os.path.join(dirpath, filename)
            total_size += os.path.getsize(file_path)
    return total_size


def get_unique_file_name(file_name, extension):
    return "{0}_{1}.{2}".format(file_name, str(time.time()).split(".")[0], extension)


def is_user_admin(self) -> bool:
    """
    Check the current process permissions as the user is was run with, and check if it is an admin user.
    Warning: The inner function fails unless you have Windows XP SP2 or
    higher. The failure causes a traceback to be printed and this
    function to return False.
    :return: True if the current user is an 'Admin' whatever that means (root on Unix), otherwise False.
    """
    if os.name == 'nt':
        import ctypes
        # WARNING: requires Windows XP SP2 or higher!
        try:
            return ctypes.windll.shell32.IsUserAnAdmin()
        except:
            traceback.print_exc()
            self.logger.warn("Admin check failed, assuming not an admin.")
            return False
    else:
        # Check for root on Posix
        return os.getuid() == 0

def convert_hex_to_int(hex_string):
    return int(hex_string, 16)