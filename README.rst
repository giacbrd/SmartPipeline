SmartPipeline
-------------

A framework for rapid development of robust data pipelines following a simple design pattern

.. figure:: https://imgs.xkcd.com/comics/data_pipeline.png
   :alt: pipeline comic

   from https://xkcd.com

.. image:: https://readthedocs.org/projects/smartpipeline/badge/?version=stable
   :target: https://smartpipeline.readthedocs.io/en/stable/?badge=stable
   :alt: Documentation Status

.. image:: https://github.com/giacbrd/SmartPipeline/actions/workflows/tests.yml/badge.svg?branch=master
   :target: https://github.com/giacbrd/SmartPipeline/actions/workflows/tests.yml
   :alt: Tests

.. image:: https://coveralls.io/repos/github/giacbrd/SmartPipeline/badge.svg?branch=master
   :target: https://coveralls.io/github/giacbrd/SmartPipeline?branch=master
   :alt: Tests Coverage


.. documentation-marker

SmartPipeline gives you the tools to design simple data pipelines,
in which each task is encapsulated in a pipeline stage.

It is straightforward to implement a pipeline,
but stages are deeply customizable:
they follow a producer-consumer pattern and can run concurrently to scale heavy tasks,
they can process batches of items at once,
and executions and errors can be monitored easily.

It is a good fit for fast and clean data analysis prototypes (small/medium projects and POCs),
as well as production code, as an alternative to plain scripts.
Use it when big task queues and workflow frameworks are overkill.
No dependencies are required.

Install
~~~~~~~

Install from PyPI, no dependencies will be installed:

.. code-block:: bash

   pip install smartpipeline

Writing your pipeline
~~~~~~~~~~~~~~~~~~~~~

SmartPipeline is designed to help the developer follow best practices,
the design is based on industrial experience with data products.

SmartPipeline focuses on simplicity and efficiency in handling data locally,
i.e. serialization and copies of the data are minimized.

Main features:

- Define a pipeline as a sequence of stateful stages,
  optionally with a source the pipeline iterates over.
- Run the pipeline over the source, indefinitely if needed,
  or use it to process single items.
- Set concurrency independently for each stage,
  and process single items asynchronously.
- Design stages that process batches, i.e. groups of consecutive items, at once.
- Customize error handling for logging and monitoring at stage level.

For example, this pipeline retrieves news from a feed
and generates text embeddings of the page contents.
We define the source of the data and two stages, then we build and run the pipeline.

.. code-block:: python

    import re

    import feedparser
    import requests
    from sentence_transformers import SentenceTransformer

    from smartpipeline.item import Item
    from smartpipeline.pipeline import Pipeline
    from smartpipeline.stage import BatchStage, Source, Stage


    class FeedReader(Source):
        def __init__(self):
            feed = feedparser.parse("https://hnrss.org/newest")
            self.urls = (entry.link for entry in feed.entries)

        # pop method generates a new data item when called
        def pop(self):
            # each call of pop consumes a URL to send to the pipeline
            url = next(self.urls, None)
            if url is not None:
                item = Item()
                item.data["url"] = url
                return item
            # when all URLs are consumed we stop the pipeline
            else:
                self.stop()


    class NewsRetrieve(Stage):
        def process(self, item):
            # add the page content to each item,
            # HTTP errors are handled by the pipeline error manager
            html = requests.get(item.data["url"]).text
            item.data["content"] = re.sub('<.*?>', '', html).strip()
            return item


    class NewsEmbedding(BatchStage):
        def __init__(self, size: int):
            super().__init__(size)
            self.model = SentenceTransformer("all-MiniLM-L6-v2")

        def process_batch(self, items):
            # efficiently compute embeddings by batching page texts,
            # instead of processing one page at a time
            vectors = self.model.encode([item.data["content"] for item in items])
            for vector, item in zip(vectors, items):
                item.data["vector"] = vector
            return items


    pipeline = (
        Pipeline()
        .set_source(FeedReader())
        # with threads (the default) we speed up the multiple HTTP calls
        .append("retriever", NewsRetrieve(), concurrency=4)
        # each batch of items to vectorize will be of size 10
        .append("vectorizer", NewsEmbedding(size=10))
        .build()
    )


    for item in pipeline.run():
        print(item)

`Read the documentation <https://smartpipeline.readthedocs.io>`_ for the full guide.

The `examples` folder contains full working sample pipelines.

AI agents note
~~~~~~~~~~~~~~

Since October 2026 the codebase development is supported by LLM code agents,
strictly using open-source tools and open-weight or free models,
always human reviewed.