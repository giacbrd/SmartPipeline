Developer Interface
===================

Source and Stages
-----------------

Base abstract classes for defining a pipeline source and processing stages.

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

The unit of data flowing through a pipeline.

.. autoclass:: smartpipeline.item.Item
   :inherited-members:
   :members:

Pipeline
--------

Main class for assembling stages and executing the pipeline, along with its
stage specification and lifecycle types.

.. autoclass:: smartpipeline.pipeline.Pipeline
   :inherited-members:
   :members:

.. autoclass:: smartpipeline.pipeline.StageSpec
   :members:

.. autoclass:: smartpipeline.pipeline.PipelineState
   :members:

.. autoclass:: smartpipeline.pipeline.PipelineStateError
   :members:

Error Handling
--------------

Exception classes raised during stage execution and managers for handling and
retrying errors.

.. autoclass:: smartpipeline.error.exceptions.Error
   :inherited-members:
   :members:

.. autoclass:: smartpipeline.error.exceptions.SoftError
   :inherited-members:
   :members:

.. autoclass:: smartpipeline.error.exceptions.CriticalError
   :inherited-members:
   :members:

.. autoclass:: smartpipeline.error.exceptions.RetryError
   :inherited-members:
   :members:

.. autoclass:: smartpipeline.error.exceptions.BatchOutputError
   :inherited-members:
   :members:

.. autoclass:: smartpipeline.error.handling.ErrorManager
   :inherited-members:
   :members:

.. autoclass:: smartpipeline.error.handling.RetryManager
   :inherited-members:
   :members:

Helpers
-------

Built-in helper sources and item subclasses shipped with the library.

.. autoclass:: smartpipeline.helpers.LocalFilesSource
   :inherited-members:
   :members:

.. autoclass:: smartpipeline.helpers.FilePathItem
   :inherited-members:
   :members:
