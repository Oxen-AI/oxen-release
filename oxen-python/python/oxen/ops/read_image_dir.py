import oxen
from pathlib import Path

class ReadImageDir(oxen.Op):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def call(self, args):
        # args[0]: path to directory
        # args[1]: series of paths to images 
        for path in args[1]:
            print(path)
