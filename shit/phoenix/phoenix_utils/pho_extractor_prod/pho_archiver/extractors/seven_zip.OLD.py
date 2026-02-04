import os
import shutil
import subprocess
from abc import ABC
from io import BytesIO
from zipfile import ZipInfo
from pho_archiver.exceptions import EmptyArchive, DamagedArchive, PasswordRequired, ZipBomb, CorruptedArchive
from pho_archiver.extractors.helpers import ExtractHelper, GeneralHelper
from pho_archiver.repaire_zip import RepairZip
from pho_globals import Extract


class SevenZipUtility(Extract, ABC):
    def __init__(self, logger, config):
        super().__init__(logger, config)
        self.logger = logger
        self.config = config
        self.seven_zip = self.config.get('7z_path')

    def _try_find_password(self, input_file, output_directory):
        """
        try to open the archive password with brute force
        :param input_file: the archive
        :param output_directory: the directory that the archive will be extracted into
        :return: the function that extract the archive
        """
        password = ""
        passwords = self.config.get("password_list", [])

        if not passwords:
            self.logger.warning("There are no passwords in the configuration for brute-force.")
            return []

        for password in passwords:
            try:
                command = [self.seven_zip, 't', input_file, f"-p{password}"]
                # this command checks if the password is correct
                GeneralHelper.run_command(command)
            except PasswordRequired:
                pass
            except Exeption as e:
                self.logger.error(f"There is an error in extracting the archive {input_file}: {e}")
                return []

        return self.extract_file(input_file, output_directory, password=password)

    def _try_fix_archive(self, input_file, chunk_size, output_directory, password):
        """
        Try fixing a corrupt archive
        :param input_file: the archive
        :param chunk_size: the size of the chunks the files are read in
        :param output_directory: the directory that the archive will be extracted into
        :param password: password
        :return: the function that extract the archive
        """
        try:
            # Write the out_data to a file or use it as needed
            file_name, file_type = os.path.basename(input_file).split('.')
            repaired_file_name = "repaired_{}.{}".format(file_name, file_type)

            temp_path = os.path.join("working_directory", file_name)
            os.makedirs(temp_path, exist_ok=True)
            repaired_file_path = os.path.join(temp_path, repaired_file_name)

            with RepairZip(input_file, mode="r") as rz:
                # attempt to fix
                if not rz.fix_zip():  # if fails
                    raise

                # Initialize RepairZip with the in-memory buffer
                with RepairZip(repaired_file_path, "w") as zo:
                    for path in rz.namelist():
                        # Read each file from the original RepairZip instance
                        file_data_buffer = BytesIO()
                        with rz.open(path) as file:
                            while True:
                                chunk = file.read(chunk_size)
                                if not chunk:
                                    break
                                file_data_buffer.write(chunk)

                            zinfo = ZipInfo(path)
                            zinfo.compress_type = rz.ZIP_DEFLATED
                            zo.writestr(zinfo, file_data_buffer.getvalue())

                return self.extract_file(repaired_file_path, output_directory, True, password)

        except Exception as e:
            self.logger.error(f"The archive {input_file} is corrupted. Failed to fix. {e}")
            return []

    def extract_file(self, input_file, output_directory, already_tried=False, password=None):
        """
        Extract the archive
        :param already_tried: does it is the firs time trying to extract
        :param input_file: the archive
        :param output_directory: the directory that the archive will be extracted into
        :param password: password
        :return: a list of the archive's files
        """
        file_type = ''
        temp_path = ""
        file_paths = []
        chunk_size = self.config.get('chunk_size')
        try:
            if not GeneralHelper.validate_input(file=input_file, directory=output_directory):
                return

            file_name, file_type = os.path.splitext(os.path.basename(input_file))
            os.makedirs(output_directory, exist_ok=True)

            # Extract the file with the specified password if necessary
            command = [self.seven_zip, 'x', '-y', input_file, f'-o{output_directory}']

            # Some UDF samples were wrongly identified as plain ISO by 7z.
            # By adding the .iso extension, it somehow made 7z identify it as UDF.
            # Our Identify was also identifying it as "iso", so we can't only rely on "archive/udf".
            if file_type in [".udf", ".iso"]:
                temp_path = os.path.join("working_directory", file_name, f"renamed_iso.iso")
                os.makedirs(os.path.dirname(temp_path), exist_ok=True)
                shutil.copy2(input_file, temp_path)
                command[-2] = temp_path

            if password:
                command.extend(['-p' + password])

            file_paths, total_size = ExtractHelper.get_files_inside_archive(input_file, output_directory, self.seven_zip)

            if total_size == 0:
                # check if the file is empty
                raise EmptyArchive(input_file)

            ExtractHelper.check_zip_bomb(input_file, file_paths, total_size, self.config.get("max_depth"),
                                         self.config.get('max_file_count'), self.config.get('max_size_bytes'))

            GeneralHelper.run_command(command)

            # Check file integrity
            for file in file_paths:
                if not os.path.exists(file):
                    raise DamagedArchive(f'Extracted file may be damaged. The file {file} is missing.')

        except subprocess.CalledProcessError as e:
            self.logger.error(f"There is an error when trying to extract the archive: {input_file}:"
                              f" {e.stderr.decode()}")
            file_paths = []
        except DamagedArchive as e:
            self.logger.error(e)
            file_paths = []
        except ZipBomb as e:
            self.logger.error(f"There is a chance for 'zip bomb': {e}")
            file_paths = []
        except CorruptedArchive as e:
            if not already_tried:
                self.logger.info(f"The archive {input_file} is corrupted: {e}. Trying to fix.")
                file_paths = self._try_fix_archive(input_file, chunk_size, output_directory, password)
            else:
                self.logger.error(f"Failed to repair the file {input_file}.")
                file_paths = []
        except PasswordRequired:
            if not password:
                file_paths = self._try_find_password(input_file, output_directory)
            else:
                self.logger.error(f"Failed to found the password for file {input_file}.")
                file_paths = []
        except EmptyArchive as e:
            self.logger.info(str(e))
            file_paths = True
        except Exception as e:
            self.logger.error(f"There is an error in extracting the archive {input_file}: {e}")
            file_paths = []

        finally:
            # clean temp folders
            if file_type in [".udf", ".iso"] and os.path.exists(temp_path):
                shutil.rmtree(os.path.dirname(temp_path))
            if already_tried:
                shutil.rmtree(os.path.dirname(input_file))
            if os.path.exists("working_directory") and len(os.listdir("working_directory")) == 0:
                os.rmdir("working_directory")
            return file_paths
