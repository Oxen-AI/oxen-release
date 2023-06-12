Image Classification Loader
===========================
.. autoclass:: oxen.loaders.ImageClassificationLoader
   :members:

   .. automethod:: __init__
   

Usage
-----


.. code-block:: python


    from oxen import LocalRepo
    from oxen.loaders import ImageClassificationLoader

    repo = LocalRepo()

    # Demo data for supervised image classification
    repo.clone("https://hub.oxen.ai/ba/dataloader-images")

    loader = ImageClassificationLoader(
        imagery_root_dir = repo.path,
        label_file = f"{repo.path}/annotations/labels.txt",
        df_file = f"{repo.path}/annotations/train.csv",
        path_name = "file",
        label_name = "hair_color"
    )

    X_train, y_train, mapper = loader.run()
