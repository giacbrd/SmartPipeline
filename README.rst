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

SmartPipeline gives you the tools to design and formalize simple data pipelines,
in which tasks are sequentially encapsulated in pipeline stages.

It is straightforward to implement pipelines,
but they are deeply customizable:
stages can run concurrently and scale on heavy tasks,
they can process batch of items at once,
moreover executions and errors can be monitored easily.

It is a framework for engineering sequences of data operations
and making them concurrent, following an optimized but transparent producer-consumer pattern.
An excellent solution for fast and clean data analysis prototypes (small/medium projects and POC)
but also for production code, as an alternative to plain scripts.
Consider it as a solution for problems where big task queues and workflow frameworks are overkill.
No dependencies are required.

Install
~~~~~~~

Install from PyPI, no dependencies will be installed:

.. code-block:: bash

   pip install smartpipeline

Writing your pipeline
~~~~~~~~~~~~~~~~~~~~~

SmartPipeline is designed to help the developer following best practices,
the design is based on industrial experience on data products.

SmartPipeline focuses on simplicity and efficiency in handling data locally,
i.e. serialization and copies of the data are minimized.

Main features:

- Define a pipeline object as a sequence of stateful stage objects,
  optionally set a source on which the pipeline iterates.
- A pipeline can run indefinitely on the source or it can be used to process single items.
- Concurrency can be set independently for each stage and single items can be processed asynchronously.
- A stage can be designed for processing batches, i.e. sequences of consecutive items, at once.
- Custom error handling can be set for logging and monitoring at stage level.

An example of a trivial pipeline for retrieving news from a feed
and generating text embeddings of the raw pages content.
We define the source of the data and two stages, then we build and run the pipeline.

.. code-block:: python

    class FeedReader(Source):
        def __init__(self):
            feed = feedparser.parse("https://hnrss.org/newest")
            self.urls = (entry.link for entry in feed.entries)

        # pop method generates a new data item when called
        def pop(self):
            # each call of pop consumes an url to send to the pipeline
            url = next(self.urls, None)
            if url is not None:
                item = Item()
                item.data["url"] = url
                return item
            # when all urls are consumed we stop the pipeline
            else:
                self.stop()


    class NewsRetrieve(Stage):
        def process(self, item):
            # add the page content to each item,
            # http errors will be implicitly handled by the pipeline error manager
            html = requests.get(item.data["url"]).text
            item.data["content"] = re.sub('<.*?>', '', html).strip()
            return item


    class NewsEmbedding(BatchStage):
        def __init__(self, size: int):
            super().__init__(size)
            self.model = SentenceTransformer("all-MiniLM-L6-v2")

        def process_batch(self, items):
            # efficiently compute embeddings by batching pages texts,
            # instead of processing one page at a time
            vectors = self.model.encode([item.data["content"] for item in items])
            for vector, item in zip(vectors, items):
                item.data["vector"] = vector
            return items


    pipeline = (
        Pipeline()
        .set_source(FeedReader())
        # by using multi-thread (default) concurrency we speed up multiple http calls
        .append("retriever", NewsRetrieve(), concurrency=4)
        # each batch of items to vectorize will be of size 10
        .append("vectorizer", NewsEmbedding(size=10))
        .build()
    )


    for item in pipeline.run():
        print(item)

`Read the documentation <https://smartpipeline.readthedocs.io>`_ for an exhaustive guide.

The `examples` folder contains full working sample pipelines.

Future improvements:

- Stages can be memory profiled.
- Processed items can be cached at stage level.