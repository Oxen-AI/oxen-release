Chatbot Loader 
===============

.. autoclass:: oxen.loaders.ChatLoader
    :members:

    .. automethod:: oxen.loaders.ChatLoader.__init__

Usage
-----


.. code-block:: python


    from oxen import LocalRepo
    from oxen.loaders import ChatLoader

    repo = LocalRepo()

    # Demo data for supervised image classification
    repo.clone("https://hub.oxen.ai/ba/dataloader-chat")

    loader = ChatLoader(
        prompt_file = f"{repo.path}/prompt.txt",
        data_file = f"{repo.path}/examples.tsv", 
    )

    [chat_df] = loader.run()
