Developer Interface
===================

Source and Stages
-----------------

Base abstract classes for pipeline source and stages

.. autoclass:: smartpipeline.stage.Source
   :inherited-members:
   :members:

.. autoclass:: smartpipeline.stage.Stage
   :inherited-members:
   :members:

.. autoclass:: smartpipeline.stage.BatchStage
   :inherited-members:
   :members:

Data Items
----------

The unit of data in a pipeline

.. autoclass:: smartpipeline.item.Item
   :inherited-members:
   :members:

Pipeline
--------

Main class for designing the sequence of stages and execute them

.. autoclass:: smartpipeline.pipeline.Pipeline
   :inherited-members:
   :members:

Error Handling
-----------------

Exceptions to generate in case of errors and how to handle them

.. autoclass:: smartpipeline.error.exceptions.SoftError
   :inherited-members:
   :members:

.. autoclass:: smartpipeline.error.exceptions.CriticalError
   :inherited-members:
   :members:

.. autoclass:: smartpipeline.error.handling.ErrorManager
   :inherited-members:
   :members:
