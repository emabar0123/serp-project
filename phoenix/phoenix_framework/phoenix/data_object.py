import json
from enum import Enum


class SuccessEnumAsString(str, Enum):
    Finished = "True",
    Failed = "False",
    NotCompatibale = "NotCompatibale"


class BaseMetadata(dict):
    def __init__(self, **kwargs):
        super(BaseMetadata, self).__init__(**kwargs)

    def __setattr__(self, key, value):
        self[key] = value
        return dict.__setattr__(self, key, value)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as e:
            raise AttributeError(e)

    def __keytransform__(self, key):
        return key

    __delattr__ = dict.__delattr__

    def __delitem__(self, key):
        if hasattr(self, key):
            delattr(self, key)
        return dict.__delitem__(self, self.__keytransform__(key))

    def __getitem__(self, key):
        return dict.__getitem__(self, self.__keytransform__(key))

    def __setitem__(self, key, value):
        return dict.__setitem__(self, self.__keytransform__(key), value)

    def __contains__(self, key):
        return dict.__contains__(self, self.__keytransform__(key))

    def __str__(self):
        return json.dumps(self)


class ResultMetadata(BaseMetadata):
    def __init__(self, **kwargs):
        super(ResultMetadata, self).__init__(**kwargs)
        self.DataType = kwargs.get("DataType", "")
        self.ToolType = kwargs.get("ToolType", "")
        self.SourceType = kwargs.get("SourceType", "")
        self.ResultType = kwargs.get("ResultType", "")
        self.Path = kwargs.get("Path", "")
        self.ScanId = kwargs.get("ScanId")
        self.ModuleDisplayName = kwargs.get("ModuleDisplayName", "")
        self.Extension = kwargs.get("Extension", "")
        self.Application = kwargs.get("Application", "")
        self.Module = kwargs.get("Module", "")
        self.Version  = kwargs.get("Version ", "")
        self.Success = kwargs.get("Success ", "")


class InputMetadata(BaseMetadata):
    def __init__(self, **kwargs):
        super(InputMetadata, self).__init__(**kwargs)
        self.InputId = kwargs.get("InputId")
        self.GUID = kwargs.get("GUID", "")
        self.CreatedTime = kwargs.get("CreatedTime")
        self.Operation = kwargs.get("Operation", "")
        self.Host = kwargs.get("Host", "")
        self.InputName = kwargs.get("InputName", "")
        self.InputType = kwargs.get("InputType", "")
        self.Path = kwargs.get("Path", "")
        self.Size = kwargs.get("Size")
        self.Index = kwargs.get("Index", "")
        self.SourceApplication = kwargs.get("SourceApplication", "test")


class Hash(BaseMetadata):
    def __init__(self, **kwargs):
        super(Hash, self).__init__(**kwargs)
        self.SHA512 = kwargs.get("SHA512", "")
        self.SHA256 = kwargs.get("SHA256", "")
        self.SHA1 = kwargs.get("SHA1", "")
        self.MD5 = kwargs.get("MD5", "")


class Root(BaseMetadata):
    def __init__(self, **kwargs):
        super(Root, self).__init__(**kwargs)
        self.Name = kwargs.get("Name", "")
        self.Hash = Hash(**kwargs.get("Hash", {}))


class JobMetadata(BaseMetadata):
    def __init__(self, **kwargs):
        super(JobMetadata, self).__init__(**kwargs)
        self.JobId = kwargs.get("JobId")
        self.RequestedModules = kwargs.get("RequestedModules", [])
        self.WriteResults = kwargs.get("WriteResults", "")
        self.Classified = kwargs.get("Classified", False)
        self.Priority = kwargs.get("Priority", 5)
        self.ForceRun = kwargs.get("ForceRun", False)


class FileMetadata(BaseMetadata):
    def __init__(self, **kwargs):
        super(FileMetadata, self).__init__(**kwargs)
        self.Path = kwargs.get("Path", "")
        self.FileType = kwargs.get("FileType", "")
        self.LogicalPath = kwargs.get("LogicalPath", "")
        self.VolumeSnapshot = kwargs.get("VolumeSnapshot", "")
        self.Hash = Hash(**kwargs.get("Hash", {}))
        self.Size = kwargs.get("Size")
        self.Extracted = kwargs.get("Extracted", False)
        self.Root = Root(**kwargs.get("Root", {}))
        self.Parent = Root(**kwargs.get("Parent", {}))
        self.ExtractionLevel = kwargs.get("ExtractionLevel", 0)


class message(BaseMetadata):
    def __init__(self, **kwargs):
        super(message, self).__init__(**kwargs)
        if kwargs.get("InputMetadata", {}) is not None:
            self["InputMetadata"] = InputMetadata(**kwargs.get("InputMetadata", {}))
        else:
            self["InputMetadata"] = InputMetadata(**{})
        if kwargs.get("JobMetadata", {} is not None):
            self["JobMetadata"] = JobMetadata(**kwargs.get("JobMetadata", {}))
        else:
            self["JobMetadata"] = JobMetadata(**{})
        if kwargs.get("FileMetadata", {}) is not None:
            self["FileMetadata"] = FileMetadata(**kwargs.get("FileMetadata", {}))
        else:
            self["FileMetadata"] = FileMetadata(**{})
        if kwargs.get("ResultMetadata", {}) is not None:
            self["ResultMetadata"] = ResultMetadata(**kwargs.get("ResultMetadata", {}))
        else:
            self["ResultMetadata"] = ResultMetadata(**{})