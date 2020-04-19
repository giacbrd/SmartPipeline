Developer Interface
===================

Source and Stages
-----------------

Classes for defining a pipeline source and its stages with specific processing methods

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

.. autoclass:: smartpipeline.item.DataItem
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

.. autoclass:: smartpipeline.error.exceptions.Error
   :inherited-members:
   :members:

.. autoclass:: smartpipeline.error.exceptions.CriticalError
   :inherited-members:
   :members:

.. autoclass:: smartpipeline.error.handling.ErrorManager
   :inherited-members:
   :members:
