Regression Loader
===========================

.. autoclass:: oxen.loaders.RegressionLoader
   :members:

   .. automethod:: __init__
   

Usage
-----


.. code-block:: python


    from oxen import LocalRepo
    from oxen.loaders import RegressionLoader

    repo = LocalRepo()

    # Demo data for supervised image classification
    repo.clone("https://hub.oxen.ai/ba/dataloader-regression")

    loader = RegressionLoader(
        data_file = f"{repo.path}/prices.csv",
        pred_name = "price",
        f_names = ["sqft", "num_bed", "num_bath"]
    )

    X, y = loader.run()
