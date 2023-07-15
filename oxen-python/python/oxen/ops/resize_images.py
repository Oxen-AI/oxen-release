import oxen
import numpy as np
import cv2
from tqdm import tqdm


class ResizeImages(oxen.Op):
    """
    Resizes a list of images to a common size for use in computer vision tasks.

    Args:
        args[0]: List[np.ndarray]
            List of images to resize (height, width, channels)
        args[1]: int | None
            Height and width dimension for cropping square images
        args[2]: str
            Method for resizing images to square size. Options are
            "crop", "pad", and "squeeze".
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def crop(self, image, size):
        # Resize such that shortest side is target size
        if image.shape[0] < image.shape[1]:
            resized = self._resize_same_aspect(image, height=size)
        else:
            resized = self._resize_same_aspect(image, width=size)
        result = self._center_crop(resized, size, size)
        return result.astype(image.dtype)

    def pad(self, image, size, inter=cv2.INTER_LINEAR):
        # Resize largest dimension to target size
        if image.shape[0] < image.shape[1]:
            resized = self._resize_same_aspect(image, width=size, inter=inter)
        else:
            resized = self._resize_same_aspect(image, height=size, inter=inter)

        color = (0, 0, 0)
        result = np.full((size, size, image.shape[2]), color)

        old_height, old_width = resized.shape[:2]
        x_center = (size - old_width) // 2
        y_center = (size - old_height) // 2

        # copy img image into center of result image
        result[
            y_center : y_center + old_height, x_center : x_center + old_width
        ] = resized

        return result.astype(image.dtype)

    def squeeze(self, image, size, inter=cv2.INTER_LINEAR):
        result = cv2.resize(image, (size, size), interpolation=inter)
        return result.astype(image.dtype)

    def _resize_same_aspect(
        self, image, height=None, width=None, inter=cv2.INTER_LINEAR
    ):
        dim = None
        (h, w) = image.shape[:2]

        if width is None and height is None:
            return image

        if width is None:
            r = height / float(h)
            dim = (int(w * r), height)
        else:
            r = width / float(w)
            dim = (width, int(h * r))

        result = cv2.resize(image, dim, interpolation=inter)
        return result

    def _center_crop(self, image, out_height, out_width):
        height, width = image.shape[:2]
        startx = width // 2 - out_width // 2
        starty = height // 2 - out_height // 2

        if len(image.shape) > 2:
            return image[starty : starty + out_height, startx : startx + out_width, :]
        else:
            return image[starty : starty + out_height, startx : startx + out_width]

    def call(self, args):
        if args[1] is None:
            return np.array(args[0])

        n_channels = args[0][0].shape[2]
        out_dtype = args[0][0].dtype
        result = np.zeros(
            (
                len(args[0]),
                args[1],
                args[1],
                n_channels,
            ),
            dtype=out_dtype,
        )

        print("Resizing images...")
        for i in tqdm(range(len(args[0]))):
            if args[2] == "crop":
                modified = self.crop(args[0][i], args[1])
            elif args[2] == "pad":
                modified = self.pad(args[0][i], args[1])
            elif args[2] == "squeeze":
                modified = self.squeeze(args[0][i], args[1])
            else:
                raise ValueError(f"Invalid argument {args[2]} for resize_method")
            result[i] = modified
        return result
