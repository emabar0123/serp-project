from pho_globals import Extract
from abc import ABC
import os
from pho_archiver.extractors.helpers import GeneralHelper


class MobiUtility(Extract, ABC):
    def __init__(self, logger, config):
        super().__init__(logger, config)
        self.logger = logger
        self.config = config
        self.mobi_tool = self.config.get('mobi_path', "")
        if os.path.exists(self.mobi_tool) is None:
            raise FileNotFoundError("mobi tool is not installed or not in the system path.")

    def extract_file(self, input_file, output_directory, already_tried=False, password=None):
        """
        Extract the archive
        :param already_tried: does it is the firs time trying to extract
        :param input_file: the archive
        :param output_directory: the directory that the archive will be extracted into
        :param password: password. There is no password possibility in mobi.
        :return: a list of the archive's files
        """
        file_paths = False
        try:
            if not GeneralHelper.validate_input(file=input_file, directory=output_directory):
                return

            os.makedirs(output_directory, exist_ok=True)

            # Extract the file with the specified password if necessary
            command = ["python", self.mobi_tool, input_file, output_directory]

            GeneralHelper.run_command(command)

            if os.listdir(output_directory):
                file_paths = True

        except subprocess.CalledProcessError as e:
            self.logger.error(f"There is an error when trying to extract the archive: {input_file}:"
                              f" {e.stderr.decode()}")
            file_paths = False
        except DamagedArchive as e:
            self.logger.error(e)
            file_paths = False
        except CorruptedArchive as e:
            self.logger.error(f"The file {input_file} is corrupted. {e}")
            file_paths = False
        except EmptyArchive as e:
            self.logger.info(str(e))
            file_paths = True
        except Exception as e:
            self.logger.error(f"There is an error in extracting the archive {input_file}: {e}")
            file_paths = False

        finally:
            return file_paths
