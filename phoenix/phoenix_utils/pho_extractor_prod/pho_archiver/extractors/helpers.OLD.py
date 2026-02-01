import os
import re
import subprocess
from pho_archiver.exceptions import DamagedArchive, PasswordRequired, ZipBomb, CorruptedArchive


class GeneralHelper:
    @staticmethod
    def check_config(config):
        """
        check if the configuration contain all the necessary information
        :param config: the configuration
        :return: True/ False
        """
        checking_list = ["supported_types", "mime_types_dict", "max_depth", "max_file_count",
                         "max_size_bytes", "chunk_size"]

        for check in checking_list:
            if not config.get(check, ""):
                return False, check
        return True, ""

    @staticmethod
    def combine_settings(global_settings, config):
        """
        combine global_settings and the microservice configuration
        """
        combined_json = {}
        if global_settings and global_settings:
            combined_json = global_settings.copy()
        for key, value in config.items():
            combined_json[key] = value
        return combined_json

    @staticmethod
    def validate_input(file=None, directory=None):
        """
        :param file: File path.
        :param directory: Directory path
        :return:  for file return if file exist for directory if exist and have permission
        """
        if file:
            if not os.path.isfile(file):
                raise FileNotFoundError(f'File {file} does not exist.')
        if directory:
            if not os.path.exists(directory):
                raise IsADirectoryError(f'Directory {directory} does not exist')
            if not os.access(directory, os.W_OK):
                raise PermissionError(f'Directory {directory} is not writable.')
        return True

    @staticmethod
    def run_command(command):
        """
        run cmd command
        :param command: the command
        :return: the result of the command
        """
        process = subprocess.run(command,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE,
                                 stdin=subprocess.PIPE,
                                 text=True)

        if process.returncode != 0:
            if any(s in process.stderr for s in ["password", "encrypted"]) or \
                    ("Break signaled" in process.stderr and "Enter password:" in process.stdout):
                # the password is missing or incorrect
                raise PasswordRequired
            raise CorruptedArchive(process.stderr)
        return process.stdout


class ExtractHelper:
    @staticmethod
    def check_zip_bomb(archive_path, file_paths, total_size, max_depth=10, max_file_count=1000,
                       max_size_bytes=50 * 1024 * 1024):
        """

        :param archive_path: the path to the archive
        :param file_paths: the path of files inside the archive
        :param total_size: the size of the archive
        :param max_depth: the max value of depth in files possible
        :param max_file_count: the max number of files possible
        :param max_size_bytes: the max size file possible
        :return: true
        """
        if len(file_paths) > max_file_count:
            raise ZipBomb(f"The number of files inside the archive: {archive_path} is higher than {max_file_count}.")

        if total_size > max_size_bytes:
            raise ZipBomb(f"The uncompressed size inside the archive: {archive_path} is higher than {max_size_bytes}.")

        problematic_files = []
        for file in file_paths:
            if file.count('/') + file.count('\\') > max_depth:
                problematic_files.append(file)
        if problematic_files:
            raise ZipBomb(f"The depth of the files {str(problematic_files)} inside the archive: {archive_path} is "
                          f"higher than {max_depth}.")

        return True

    @staticmethod
    def get_files_inside_archive(archive_path, extract_to_dir, seven_zip, password=None):
        """
        get the files inside an archive
        :param seven_zip: path to seven zip extractor
        :param archive_path: the path to the archive
        :param extract_to_dir: directory to extract the files to
        :param password: password
        :return: list of files that were extracted from the archive
        """
        command = [seven_zip, 'l', '-ba', archive_path]
        if password:
            command.append('-p' + password)

        output = GeneralHelper.run_command(command)

        if not output:
            # There are no files, or it is not possible to extract them
            return [], 0

        file_paths = []
        folders = []
        problematic_files = []
        total_size = 0
        for line in output.splitlines():
            uncompressed_size, file_name = re.split(r'\s{2,}', line)[-2:]  # get the path of the file and its size
            file_name = file_name.replace('.\\', '').replace('./', '')
            if uncompressed_size.isdigit() and int(uncompressed_size) >= 0:
                total_size += int(uncompressed_size)
                file_paths.append(file_name)
            elif isinstance(uncompressed_size, str) and 'D..' in uncompressed_size:
                # it is a folder
                folders.append(file_name)
            else:
                # The file has a problem
                problematic_files.append(file_name)
        if problematic_files:
            raise DamagedArchive(f"Could not find the size of files: {str(problematic_files)}. There is a problem with "
                                 f"them.")

        for folder in folders:
            # **CVE-2023-38831** a vulnerability when zip archive include a safe file and a folder with the exact same
            # name(including the '.txt' for example) that contain malicious files.
            if any(file.startswith(folder) for file in file_paths):
                raise DamagedArchive(
                    f"The archive {archive_path} contain malicious folder {folder}. Risk of CVE-2023-38831")

        if not file_paths:
            # There are no files in the archive
            return [], 0

        return [os.path.join(extract_to_dir, file) for file in file_paths], total_size
