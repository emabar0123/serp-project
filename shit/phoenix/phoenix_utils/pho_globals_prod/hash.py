import hashlib
import os

read_flag = "rb"


def calc_hash_directory(dir_path):
    item_hash = hashlib.md5()
    for root, dirs, files in os.walk(dir_path):
        for file in files:
            full_path = os.path.join(root, file)
            with open(full_path, read_flag) as disk_fd:
                data = disk_fd.read(2 ** 24)
                item_hash.update(data)
    disk_hash = item_hash.hexdigest()
    return disk_hash


def calc_hash(disk_path):
    """

    :type disk_path: str
    :param disk_path: path to disk
    :return: hash
    """

    size = os.path.getsize(disk_path)
    item_hash = hashlib.md5()
    with open(disk_path, read_flag) as disk_fd:
        try:
            count = 0
            while True:
                count += 2 ** 24
                data = disk_fd.read(2 ** 24)
                if not data or float(count) * 100 / size > 0.2:
                    break
                item_hash.update(data)
        except IOError as e:
            print(str(e))
    if count == 2 ** 24:
        item_hash.update(data)
    disk_hash = item_hash.hexdigest()
    return disk_hash


def get_file_hashes(file_path, block_size=256):
    md5_hash = sha1_hash = sha256_hash = ""
    if not file_path:
        return md5_hash, sha1_hash, sha256_hash
    md5 = hashlib.md5()
    sha1 = hashlib.sha1()
    sha256 = hashlib.sha256()
    if os.path.isfile(file_path):
        try:
            with open(file_path, 'rb') as file_handle:
                file_data = file_handle.read(block_size)
                while len(file_data) > 0:
                    md5.update(file_data)
                    sha1.update(file_data)
                    sha256.update(file_data)
                    file_data = file_handle.read(block_size)
            md5_hash = md5.hexdigest().upper()
            sha1_hash = sha1.hexdigest().upper()
            sha256_hash = sha256.hexdigest().upper()
        except (PermissionError, FileNotFoundError, OSError):
            if os.name == "nt":
                try:
                    import win32file
                    import msvcrt
                    handle = win32file.CreateFile(file_path,
                                                  win32file.GENERIC_READ,
                                                  win32file.FILE_SHARE_READ,
                                                  None,
                                                  win32file.OPEN_EXISTING,
                                                  win32file.FILE_FLAG_BACKUP_SEMANTICS | win32file.FILE_FLAG_OPEN_REPARSE_POINT,
                                                  None)
                    file_descriptor = msvcrt.open_osfhandle(handle.handle, os.O_TEXT | os.O_RDONLY)
                    with open(file_descriptor, 'rb') as file_handle:
                        file_data = file_handle.read(block_size)
                        while len(file_data) > 0:
                            md5.update(file_data)
                            sha1.update(file_data)
                            sha256.update(file_data)
                            file_data = file_handle.read(block_size)
                    try:
                        win32file.CloseHandle(handle)
                    except:
                        pass
                    md5_hash = md5.hexdigest().upper()
                    sha1_hash = sha1.hexdigest().upper()
                    sha256_hash = sha256.hexdigest().upper()
                except Exception as e:
                    pass
            else:
                pass
        except PermissionError:
            pass

    return md5_hash, sha1_hash, sha256_hash
