from typing import Dict

class Data:
    def __init__(self, ispb: str, name: str, code: str, fullname: str):
        self.ispb = ispb
        self.name = name
        self.code = code
        self.fullname = fullname

    def to_dict(self) -> Dict:
        return {
            "ispb": self.ispb,
            "name": self.name,
            "code": self.code,
            "fullname": self.fullname
        }