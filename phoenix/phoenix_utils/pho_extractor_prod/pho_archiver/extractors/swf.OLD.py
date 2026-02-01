import os
import subprocess
import shutil
from abc import ABC
from pho_archiver.exceptions import EmptyArchive, DamagedArchive, PasswordRequired, ZipBomb, CorruptedArchive
from pho_archiver.extractors.helpers import ExtractHelper, GeneralHelper
from pho_globals import Extract


class SwfUtility(Extract, ABC):
    def __init__(self, logger, config):
        super().__init__(logger, config)
        self.logger = logger
        self.config = config
        self.seven_zip = self.config.get('7z_path')
        self.swf_tool = self.config.get('swf_path', "")
        if os.path.exists(self.swf_tool) is None:
            raise FileNotFoundError("swf tool is not installed or not in the system path.")

    def extract_file(self, input_file, output_directory, already_tried=True, password=None):
        """
        Extract the archive
        :param already_tried: does it is the firs time trying to extract
        :param input_file: the archive
        :param output_directory: the directory that the archive will be extracted into
        :param password: password. There is no password possibility in swf.
        :return: a list of the archive's files
        """
        file_paths = []
        try:
            if not GeneralHelper.validate_input(file=input_file, directory=output_directory):
                return

            os.makedirs(output_directory, exist_ok=True)

            command = ['java', '-jar', self.swf_tool, '-export', 'all', output_directory, input_file]

            # in swf file this function cannot always return all the information
            file_paths, total_size = ExtractHelper.get_files_inside_archive(input_file, output_directory,
                                                                            self.seven_zip)

            if total_size == 0:
                # check if the file is empty
                raise EmptyArchive(input_file)

            ExtractHelper.check_zip_bomb(input_file, file_paths, total_size, self.config.get("max_depth"),
                                         self.config.get('max_file_count'), self.config.get('max_size_bytes'))

            GeneralHelper.run_command(command)

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
            self.logger.error(f"The archive {input_file} is probably corrupted: {e}.")
            file_paths = []
        except EmptyArchive as e:
            self.logger.info(str(e))
            file_paths = True
        except Exception as e:
            self.logger.error(f"There is an error in extracting the archive {input_file}: {e}")
            file_paths = []

        finally:
            if os.path.exists("working_directory") and len(os.listdir("working_directory")) == 0:
                os.rmdir("working_directory")
            return file_paths
