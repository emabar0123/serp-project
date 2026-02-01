import os
from magic import MagicException
import magic
from pho_archiver.extractors.helpers import GeneralHelper
from pho_archiver.extractors.seven_zip import SevenZipUtility
from pho_archiver.extractors.swf import SwfUtility
from pho_archiver.extractors.mobi import MobiUtility


class ExtractorStrategy:
    def __init__(self, logger, config, global_settings):
        self.logger = logger
        self.config = GeneralHelper.combine_settings(config.get('extractor'), global_settings.get('Extractor'))
        check, field = GeneralHelper.check_config(self.config)
        if not check:  # there is a missing field
            self.logger.error(f"There is a missing field in the configuration: {field}.")
            raise
        self.supported_types = self.config.get('supported_types')
        self.convert_description_to_mime = self.config.get('mime_types_dict')
        if os.path.exists(self.config.get('7z_path', "")) is None:
            raise FileNotFoundError("7z is not installed or not in the system path.")

    def get_file_type(self, file_path):
        # Recommend using at least the first 2048 bytes, as less can produce incorrect identification
        try:
            file_type = magic.from_buffer(open(file_path, "rb").read(2048), mime=True)
            if file_type == "application/octet-stream":
                # not able to fine the type
                definition = magic.from_buffer(open(file_path, "rb").read(2048)).lower()
                for mime_type, descriptions in self.convert_description_to_mime.items():
                    if any(item.lower() in definition for item in descriptions):
                        return mime_type
            return file_type
        except MagicException:
            raise
        except Exception as e:
            self.logger.error(f"Error during get file type: {e}")
            raise

    def extract_file(self, input_file, output_directory, password=None):
        os.makedirs(output_directory, exist_ok=True)
        mime_type = self.get_file_type(input_file)

        if self.supported_types.get(mime_type):
            type_file = self.supported_types[mime_type]
        else:
            self.logger.info(f'The file {input_file} type is not in the list of types. Trying to extract with 7zip.')
            type_file = self.supported_types.get("application/x-7z-compressed")

        extractor_class = globals().get(type_file)
        if extractor_class is None:
            raise ValueError(f"Failed to load extractor class {self.supported_types[mime_type]}")

        extractor_class_obj = extractor_class(logger=self.logger, config=self.config)
        if extractor_class_obj.extract_file(input_file, output_directory, password=password):
            return True
        return False
