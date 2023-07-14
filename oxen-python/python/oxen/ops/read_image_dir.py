import oxen
from tqdm import tqdm
import cv2


class ReadImageDir(oxen.Op):
    """
    Reads in imagery as specified by a DataFrame column of paths.

    Args:
        args[0] : str
            Root imagery directory, is prefixed to DataFrame paths
        args[1] : List[str]
            Column of paths to imagery (relative to root directory specified in args[0])
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def call(self, args):
        image_data = []
        prefix = args[0]
        print("Reading images...")
        for path in tqdm(args[1]):
            img = cv2.imread(f"{prefix}/{path}", cv2.IMREAD_UNCHANGED)
            img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
            image_data.append(img)
        return image_data
