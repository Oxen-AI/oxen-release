import os
import pytest
import torch
import tensorflow as tf
import numpy as np
from oxen.loaders import ImageClassificationLoader
from torch.utils.data import TensorDataset, DataLoader


def test_image_classification_dataloader_local(
    celeba_local_repo_fully_committed, empty_local_dir
):
    repo = celeba_local_repo_fully_committed

    train_file = os.path.join(repo.path, "annotations", "train.csv")
    label_file = os.path.join(repo.path, "annotations", "labels.txt")

    loader = ImageClassificationLoader(
        imagery_root_dir=repo.path,
        label_file=label_file,
        df_file=train_file,
        path_name="file",
        label_name="hair_color",
    )
    data, labels, mapper = loader.run()

    assert data.dtype == np.uint8

    assert data.shape == (5, 218, 178, 3), "Data not returned in expected shape"
    assert labels.shape == (5,)
    assert len(mapper.items()) == 3

    # Test ease of use with pytorch
    torch_data = TensorDataset(torch.from_numpy(data), torch.from_numpy(labels))
    torch_dl = DataLoader(torch_data, batch_size=1)
    assert len(torch_dl) == 5

    # Test ease of use with tensorflow
    dataset = tf.data.Dataset.from_tensor_slices((data, labels))

    dataset = dataset.shuffle(buffer_size=len(data))
    dataset = dataset.batch(1)

    assert len(dataset) == 5


def test_image_loader_missing_unique_label(
    celeba_local_repo_fully_committed, empty_local_dir
):
    repo = celeba_local_repo_fully_committed
    train_file = os.path.join(repo.path, "annotations", "test.csv")
    label_file = os.path.join(repo.path, "annotations", "labels.txt")

    loader = ImageClassificationLoader(
        imagery_root_dir=repo.path,
        label_file=label_file,
        df_file=train_file,
        path_name="file",
        label_name="hair_color",
    )
    with pytest.raises(ValueError) as e:
        data, labels, mapper = loader.run()

    assert "label(s) in data missing" in str(e.value)


def test_image_loader_resize_crop(
    celeba_local_repo_fully_committed, empty_local_dir, tmp_path
):
    repo = celeba_local_repo_fully_committed
    train_file = os.path.join(repo.path, "annotations", "train.csv")
    label_file = os.path.join(repo.path, "annotations", "labels.txt")

    loader = ImageClassificationLoader(
        imagery_root_dir=repo.path,
        label_file=label_file,
        df_file=train_file,
        path_name="file",
        label_name="hair_color",
        resize_to=512,
        resize_method="crop",
    )

    data, labels, mapper = loader.run()

    assert data.dtype == np.uint8

    np.save(tmp_path / "imgs.npy", data)
    assert data.shape == (5, 512, 512, 3), "Data not returned in expected shape"


def test_image_loader_resize_squeeze(
    celeba_local_repo_fully_committed, empty_local_dir, tmp_path
):
    repo = celeba_local_repo_fully_committed
    train_file = os.path.join(repo.path, "annotations", "train.csv")
    label_file = os.path.join(repo.path, "annotations", "labels.txt")

    loader = ImageClassificationLoader(
        imagery_root_dir=repo.path,
        label_file=label_file,
        df_file=train_file,
        path_name="file",
        label_name="hair_color",
        resize_to=252,
        resize_method="squeeze",
    )
    data, labels, mapper = loader.run()
    np.save(tmp_path / "imgs.npy", data)
    print("this this dtype is", data.dtype)
    assert data.shape == (5, 252, 252, 3), "Data not returned in expected shape"


def test_image_loader_resize_pad(
    celeba_local_repo_fully_committed, empty_local_dir, tmp_path
):
    repo = celeba_local_repo_fully_committed
    train_file = os.path.join(repo.path, "annotations", "train.csv")
    label_file = os.path.join(repo.path, "annotations", "labels.txt")

    loader = ImageClassificationLoader(
        imagery_root_dir=repo.path,
        label_file=label_file,
        df_file=train_file,
        path_name="file",
        label_name="hair_color",
        resize_to=111,
        resize_method="pad",
    )
    data, labels, mapper = loader.run()
    np.save(tmp_path / "imgs.npy", data)
    print("this dtype is", data.dtype)
    assert data.shape == (5, 111, 111, 3), "Data not returned in expected shape"


def test_bad_resize_method(
    celeba_local_repo_fully_committed, empty_local_dir, tmp_path
):
    repo = celeba_local_repo_fully_committed
    train_file = os.path.join(repo.path, "annotations", "train.csv")
    label_file = os.path.join(repo.path, "annotations", "labels.txt")

    with pytest.raises(ValueError) as e:
        print(e)
        loader = ImageClassificationLoader(
            imagery_root_dir=repo.path,
            label_file=label_file,
            df_file=train_file,
            path_name="file",
            label_name="hair_color",
            resize_to=111,
            resize_method="bad_method",
        )
        data, labels, mapper = loader.run()
