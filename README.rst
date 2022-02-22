SmartPipeline
-------------

A framework for fast development of scalable data pipelines following a simple design pattern

.. figure:: https://imgs.xkcd.com/comics/data_pipeline.png
   :alt: pipeline comic

   from https://xkcd.com

.. image:: https://readthedocs.org/projects/smartpipeline/badge/?version=stable
   :target: https://smartpipeline.readthedocs.io/en/stable/?badge=stable
   :alt: Documentation Status

.. image:: https://github.com/giacbrd/SmartPipeline/actions/workflows/tests.yml/badge.svg
   :target: https://github.com/giacbrd/SmartPipeline/actions/workflows/tests.yml
   :alt: Tests

.. documentation-marker

SmartPipeline gives you the tools to design and formalize simple data pipelines,
in which tasks are sequentially encapsulated in pipeline stages,
but it enriches this data processing approach with a lot of features.

It is straightforward to implement pipelines,
but they are deeply customizable:
stages can run concurrently and scale on heavy tasks,
they can process batch of items at once,
moreover executions and errors are monitored by the pipeline.

It is a framework for engineering sequences of data operations:
an optimal solution for fast and clean data analysis prototypes
(small/medium projects and POC),
that can be immediately ready for production.
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
i.e. serialization and copies of the data are minimized,
with the obvious exception of multiprocessing.

The main point is creating a ``Pipeline`` object,
setting the source of data and adding custom defined stages,
which determine the operations to perform sequentially on each data item.

- A pipeline can run indefinitely on the source or it can be used to process single items
- Stages can run concurrently, both on the source or on single asynchronously processed items
- A stage can be designed for processing batches, i.e. sequences of consecutive items, at once
- Custom error handling can be set for logging and monitoring at stage level

`Read the documentation <https://smartpipeline.readthedocs.io>`_ for an exhaustive tutorial
and examples