import oxen
import numpy as np
from tqdm import tqdm
import cv2


class ReadImageDir(oxen.Op):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def call(self, args):
        image_data = []
        prefix = args[0]
        for path in tqdm(args[1]):
            img = cv2.imread(f"{prefix}/{path}")
            image_data.append(img)
        # TODO: Todo: handle differing image shapes, or leave that to user?
        # For now, assuming all images are same shape
        return np.array(image_data)

