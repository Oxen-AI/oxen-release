import oxen
import numpy as np
from tqdm import tqdm
import cv2


class ReadImageDir(oxen.Op):
    '''
    Reads in imagery as specified by a DataFrame column of paths. 

    Args:
        args[0] : str
            Root imagery directory, is prefixed to DataFrame paths
        args[1] : List[str]
            Column of paths to imagery (relative to root directory specified in args[0])
    '''
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def call(self, args):
        image_data = []
        prefix = args[0]
        for path in tqdm(args[1]):
            img = cv2.imread(f"{prefix}/{path}", cv2.IMREAD_UNCHANGED)
            image_data.append(img)
        return np.array(image_data)

