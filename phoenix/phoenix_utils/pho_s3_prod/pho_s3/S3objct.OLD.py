from datetime import datetime


class S3Objct:
    def __init__(self, key=None, size=None, lastModified=None, typeobject=None):
        self.key: str = key
        self.size: int = size
        self.lastModified: datetime = lastModified
        self.type: str = typeobject

    def __str__(self):
        return f"Key: {self.key}, Size: {self.size}, Last Modified: {self.lastModified}, Type: {self.type} "

    def __repr__(self):
        return f"""self.key: str ={self.key} 
        self.size: int = size
        self.lastModified: datetime = lastModified
        self.type: str = type object"""
