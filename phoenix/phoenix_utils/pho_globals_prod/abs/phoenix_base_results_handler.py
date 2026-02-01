from abc import abstractmethod, ABC
from pho_globals.generators_utils import chunks
import types
import codecs
import os
import traceback
import hashlib
import time
if os.name == "nt":
    try:
        from pho_globals.priviliges_utils import PrivilegesManager
        from ctypes import create_unicode_buffer, windll, c_int64, sizeof
        import win32con
        import win32api
    except SyntaxError:
        traceback.print_exc()
        print("Failed to import Win32API modules.")


__newline__ = "\n"
CSV_SIZE = 1024 * 1024 * 1024 / 4
__encodings__ = ['utf-16LE']
__records_limit__ = 1000000


class PhoenixBaseResultHandler(ABC):

    def __init__(self, logger: object, config: object) -> None:
        """
        Abstract base class for phoenix base results handlers.
        :param logger: logging object
        :param config: configuration object
        """
        self.logger = logger
        self.config = config
        self.results_dir = None
        self.tool_path = None
        if os.name == "nt":
            self.permission_manager = PrivilegesManager()
            self.permission_manager.set_privilege('SeBackupPrivilege', True)

    def set_tool_path(self, tool_path: str) -> None:
        self.tool_path = tool_path

    @abstractmethod
    def fix_result(self, file_path: str) -> None:
        """
        This is the place to handle the parsing of the output received from the tool execution.
        The method should handle the complete handling of the output
        :param file_path: path of file to be fixed
        """
        raise NotImplementedError("You should have implement parse_result method")

    def write_parsed_output_codec(self, output_file_path: str, headers: object, output_object: object,
                                  encoding: str = "utf-16LE") -> None:
        """
        Handles writing the output given as headers and output dictionary to a csv file with encoding
        :param output_file_path: File path
        :param headers: The output headers
        :param output_object: The output records
        :param encoding: Encoding to use for the output file
        :return: Nothing
        """
        global __encodings__

        if encoding in __encodings__:
            __encodings__.remove(encoding)
        # put the requested encoding first
        __encodings__ = [encoding] + __encodings__

        try:
            if isinstance(output_object, dict):
                iterable = iter(output_object.items())
            elif isinstance(output_object, (list, types.GeneratorType)):
                iterable = output_object
            else:
                iterable = output_object

            iteration = 1
            for iterable_chunk in chunks(iterable, __records_limit__, CSV_SIZE):
                output_file_split_text = os.path.splitext(output_file_path)
                composed_output_file_path = output_file_path if iteration == 1 else "{0}-{1}{2}".format(
                    output_file_split_text[0], str(iteration),
                    output_file_split_text[1])
                for encoding in __encodings__:
                    try:
                        with codecs.open(composed_output_file_path, 'w', encoding=encoding) as output_result_handle:
                            if headers is not None:
                                output_result_handle.write(headers + __newline__)
                            for line in iterable_chunk:
                                output_result_handle.write(line + __newline__)
                        self.logger.info(
                            "Successfully wrote parsed output to codec file {0}.".format(composed_output_file_path))
                        iteration += 1
                        break
                    except Exception as ex:
                        self.write_exception_to_log(ex, "Failed to write parsed output to codec file {0}.",
                                                    composed_output_file_path)
                        continue
            self.logger.info("Successfully wrote all parsed output chunks.")
        except Exception as ex:
            self.write_exception_to_log(ex, "Failed to write parsed output to codec file {0}.", output_file_path)

    def get_output_file_path(self, original_file_path, template=None):
        output_file_path = original_file_path
        try:
            if original_file_path:
                splitted_path = os.path.splitext(original_file_path)
                if not template:
                    template = str(time.time()).split(".")[0]
                output_file_path = "{0}-{1}{2}".format(splitted_path[0], template, splitted_path[1])
        except Exception as ex:
            self.write_exception_to_log(ex, "Failed to create output file path from original path {0}.",
                                        original_file_path)
        return output_file_path

    def get_file_attributes(self, file_path):
        """
        Get the given file attributes, check all 16 available attributes
        :param file_path: File path to get attributes
        :return: String with the file attributes splitted by ;
        """
        attributes = ""
        try:
            if os.path.isfile(file_path):
                file_attributes = win32api.GetFileAttributes(file_path)

                # Check each attribute and append to the output value
                if file_attributes & win32con.FILE_ATTRIBUTE_ARCHIVE:
                    attributes += "ARCHIVE;"
                if file_attributes & win32con.FILE_ATTRIBUTE_ATOMIC_WRITE:
                    attributes += "ATOMIC_WRITE;"
                if file_attributes & win32con.FILE_ATTRIBUTE_COMPRESSED:
                    attributes += "COMPRESSED;"
                if file_attributes & win32con.FILE_ATTRIBUTE_DEVICE:
                    attributes += "DEVICE;"
                if file_attributes & win32con.FILE_ATTRIBUTE_DIRECTORY:
                    attributes += "DIRECTORY;"
                if file_attributes & win32con.FILE_ATTRIBUTE_ENCRYPTED:
                    attributes += "ENCRYPTED;"
                if file_attributes & win32con.FILE_ATTRIBUTE_HIDDEN:
                    attributes += "HIDDEN;"
                if file_attributes & win32con.FILE_ATTRIBUTE_NORMAL:
                    attributes += "NORMAL;"
                if file_attributes & win32con.FILE_ATTRIBUTE_NOT_CONTENT_INDEXED:
                    attributes += "NOT_CONTENT_INDEXED;"
                if file_attributes & win32con.FILE_ATTRIBUTE_OFFLINE:
                    attributes += "OFFLINE;"
                if file_attributes & win32con.FILE_ATTRIBUTE_READONLY:
                    attributes += "READONLY;"
                if file_attributes & win32con.FILE_ATTRIBUTE_REPARSE_POINT:
                    attributes += "REPARSE_POINT;"
                if file_attributes & win32con.FILE_ATTRIBUTE_SPARSE_FILE:
                    attributes += "SPARSE_FILE;"
                if file_attributes & win32con.FILE_ATTRIBUTE_SYSTEM:
                    attributes += "SYSTEM;"
                if file_attributes & win32con.FILE_ATTRIBUTE_TEMPORARY:
                    attributes += "TEMPORARY;"
                if file_attributes & win32con.FILE_ATTRIBUTE_VIRTUAL:
                    attributes += "VIRTUAL;"
                if file_attributes & win32con.FILE_ATTRIBUTE_XACTION_WRITE:
                    attributes += "XACTION_WRITE"

                # Remove last ";" character if exist
                if attributes[-1:] == ";":
                    attributes = attributes[:-1]
        except Exception as ex:
            self.logger.warning("Failed to retrieve file attributes, error: {0}".format(ex))
        return attributes

    def write_exception_to_log(self, ex, message_format, *values):
        """
        Write raised exception to a log object (file, center logging, ...)
        :param ex: The exception object
        :param message_format: Message format to write
        :param values: Parameters for the message format
        :return: Nothing
        """
        try:
            if values:
                self.logger.error(message_format.format(values) +" - " + str(ex))
            else:
                self.logger.error(message_format +" - " + str(ex))
        except Exception as error:
            traceback.print_exc()
            self.logger.error("Failed to log exception message.", error)
    def normalize_path(self, path):
        """
        Normalize given path so it wouldn't contain logical drive
        :param path: The path to normalize
        :return: The normalized path
        """
        fixed_path = ""
        try:
            drive, fixed_path = os.path.splitdrive(path.strip("\""))
            if fixed_path[0:1] == "\\":
                fixed_path = fixed_path[1:]
            fixed_path = "\"{0}\"".format(fixed_path)
        except Exception as ex:
            self.write_exception_to_log(ex, "Failed to parse path value {0} to drive and path for normalizing.", path)
        return fixed_path
