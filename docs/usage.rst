Guide
=====

Data items
----------

The unit of data in a pipeline is the *item*, represented by :class:`.Item` or
a subclass of it:

* :attr:`.Item.data` holds the payload: by default a dictionary (``Item()``), or
  any value passed to ``Item(data)``.
* :attr:`.Item.metadata` is a dictionary for auxiliary or temporary values that
  should stay separate from the main payload.
* :attr:`.Item.id` identifies the item. By default it looks up an ``id`` key or
  attribute on :attr:`.Item.data`, falls back to ``item.metadata["id"]``, and
  otherwise generates a UUID. Subclasses can override :attr:`.Item.id` to
  compute a custom identifier.

.. code-block:: python

    from smartpipeline import Item

    item = Item({"id": "doc-1", "text": "hello world"})
    item.metadata["source"] = "local"
    print(item.id)  # "doc-1"

As an item travels through a pipeline, it also records per-stage execution
times (:meth:`.Item.get_timing`, :attr:`.Item.timed_stages`) and any errors
raised on it (:meth:`.Item.soft_errors`, :meth:`.Item.critical_errors`).

If any stage runs on multiple processes (see :doc:`concurrency`), items are
sent across processes through queues, so both ``data`` and ``metadata`` must be
serializable with `pickle <https://docs.python.org/3/library/pickle.html>`_.

Defining the source
-------------------

A pipeline reads its input from a source set with :meth:`.Pipeline.set_source`.
For simple streams, pass any iterable or generator of :class:`.Item` instances
directly; SmartPipeline wraps it in a source that stops automatically when the
iterable is exhausted:

.. code-block:: python

    from smartpipeline import Item, Pipeline


    def number_items(total=100):
        for i in range(1, total + 1):
            item = Item({"id": i, "text": f"document {i} with code 12ab"})
            item.metadata["count"] = i
            yield item

For stateful sources (paginated APIs, database cursors, live queues), subclass
:class:`.Source` and implement :meth:`.Source.pop`. It is called repeatedly and
must return one item per call:

* Return ``None`` when no item is currently ready without ending the stream.
* Call :meth:`.Source.stop` once the source is exhausted (or leave it uncalled
  for an infinite stream).

.. code-block:: python

    import random
    import string
    from smartpipeline import Item, Source


    class RandomTextSource(Source):
        def __init__(self, total=1000):
            self._total = total
            self._counter = 0

        def pop(self):
            self._counter += 1
            if self._counter > self._total:
                self.stop()
                return None
            text = "".join(
                random.choices(string.ascii_letters + string.digits, k=80)
            )
            item = Item({"id": hash(text), "text": text})
            item.metadata["count"] = self._counter
            return item

Defining your stages
--------------------

A stage encapsulates one processing step and extends either :class:`.Stage` or
:class:`.BatchStage`.

Single-item stages
^^^^^^^^^^^^^^^^^^

Override :meth:`.Stage.process` to receive one item, enrich or transform it,
and return it:

.. code-block:: python

    import re
    from smartpipeline import SoftError, Stage


    class TextReplacer(Stage):
        def __init__(self, substitution: str):
            self._pattern = re.compile(r"\d\d\D\D")
            self._sub = substitution

        def process(self, item):
            new_text = self._pattern.sub(self._sub, item.data["text"])
            if item.data["text"] == new_text:
                # skip this stage for the item, but keep the item in the pipeline
                raise SoftError("Text has not been modified")
            item.data["text"] = new_text
            return item

Every stage provides:

* :meth:`.Stage.on_start` and :meth:`.Stage.on_end` lifecycle hooks for opening
  and closing resources (files, network clients, ML models) when the pipeline
  starts and shuts down. When a stage runs on multiple processes, these hooks
  run once inside each worker process (see :doc:`concurrency`).
* :attr:`.Stage.logger`, a dedicated standard-library ``Logger`` named after the
  stage.

Batch stages
^^^^^^^^^^^^

When an operation is faster in bulk (such as calling a remote API that accepts
lists of records or running batched model inference), subclass
:class:`.BatchStage` and override :meth:`.BatchStage.process_batch`.

Pass ``size`` (maximum items per batch) and optionally ``timeout`` (seconds to
wait before flushing a partial batch, default ``1.0``) to ``super().__init__``:

.. code-block:: python

    from smartpipeline import BatchStage


    class BatchTextNormalizer(BatchStage):
        def __init__(self, size: int = 32, timeout: float = 0.5):
            super().__init__(size=size, timeout=timeout)

        def process_batch(self, items):
            for item in items:
                item.data["text"] = item.data["text"].strip().lower()
            return items

:meth:`.BatchStage.process_batch` must return **one item for each input item**
in the batch; returning a different count raises :class:`.BatchOutputError` and
stops the pipeline. Any item that already carries a critical error from an
earlier stage automatically bypasses ``process_batch``.

Building and running a pipeline
-------------------------------

Stages are chained onto a :class:`.Pipeline` with :meth:`.Pipeline.append` and
finalized with :meth:`.Pipeline.build`:

* Each stage gets a unique ``name``.
* ``concurrency`` controls how many workers run the stage (``0`` by default for
  inline execution; ``>= 1`` for concurrent threads, or processes with
  ``executor="processes"``, see :doc:`concurrency`).
* ``stage`` can be an **instance** (created immediately and retrievable via
  :meth:`.Pipeline.get_stage`) or a **class** with optional ``init_args`` and
  ``init_kwargs`` (constructed in append order when :meth:`.Pipeline.build`
  runs).

:meth:`.Pipeline.build` validates the configuration and constructs any
class-based stages, without starting workers or calling ``on_start``.

SmartPipeline supports three ways to execute a built pipeline:

1. Iterating over a source with ``run()``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The standard mode when a source is configured. :meth:`.Pipeline.run` starts the
pipeline, yields processed items as they exit the last stage, and automatically
closes the pipeline when finished:

.. code-block:: python

    from smartpipeline import Pipeline, Stage


    class TextReducer(Stage):
        def process(self, item):
            item.data["text"] = item.data["text"][:40]
            return item


    pipeline = (
        Pipeline()
        .set_source(RandomTextSource(total=100))
        .append("text_replacer", TextReplacer(substitution="XXX"))
        .append("text_reducer", TextReducer, init_kwargs={})
        .build()
    )

    for item in pipeline.run():
        print(item.data["text"])

2. Pushing items manually with ``process_async()``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When items arrive from an external loop (e.g. a web handler or message
consumer) and you still want concurrent or batch stages, submit items with
:meth:`.Pipeline.process_async`, read results with :meth:`.Pipeline.get_item`,
and call :meth:`.Pipeline.stop` once no more items will be sent:

.. code-block:: python

    pipeline = (
        Pipeline()
        .append("text_replacer", TextReplacer(substitution="XXX"), concurrency=3)
        .append("text_reducer", TextReducer(), concurrency=1)
        .build()
    )

    for i in range(100):
        pipeline.process_async(
            Item({"id": i, "text": f"sample {i} with 99zz"}),
            callback=lambda it: it.metadata.setdefault("done", True),
        )

    for _ in range(100):
        print(pipeline.get_item().data["text"])

    pipeline.stop()

.. note::
   ``process_async`` uses worker threads/processes under the hood, not Python's
   ``asyncio``. Once ``stop()`` has been called and all output items have been
   read, further calls to :meth:`.Pipeline.get_item` raise
   :class:`.PipelineStateError`.

3. Synchronous single-item calls with ``process()``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When every stage is inline (``concurrency=0`` and no :class:`.BatchStage`),
:meth:`.Pipeline.process` runs an item directly through the stage chain and
returns it immediately, with no queues or background threads:

.. code-block:: python

    pipeline = (
        Pipeline()
        .append("text_replacer", TextReplacer(substitution="XXX"))
        .append("text_reducer", TextReducer())
        .build()
    )

    item = pipeline.process(Item({"text": "order 42ab confirmed"}))
    print(item.data["text"])
    pipeline.close()

If the pipeline has any concurrent or batch stage, :meth:`.Pipeline.process`
raises ``ValueError``.

Pipeline lifecycle and metrics
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A pipeline moves through four states (:class:`.PipelineState`):
``INITIAL -> BUILT -> RUNNING -> CLOSED``.

* After :meth:`.Pipeline.build`, stages and the error manager are frozen (adding
  stages or changing the error manager raises :class:`.PipelineStateError`),
  though :meth:`.Pipeline.set_source` is still allowed until execution starts.
* ``run()`` and ``process_async()`` are one-shot: when the run finishes, the
  pipeline transitions to ``CLOSED``. By contrast, :meth:`.Pipeline.process`
  can be called repeatedly while the pipeline is ``BUILT`` until you call
  :meth:`.Pipeline.close`.
* :meth:`.Pipeline.close` is idempotent and runs ``on_end`` on every started
  stage and the error manager.
* :attr:`.Pipeline.count`, :attr:`.Pipeline.soft_errors_count`, and
  :attr:`.Pipeline.critical_errors_count` track the items delivered by
  ``run()`` and ``get_item()`` (including across process workers).

Error handling and retries
--------------------------

Soft vs. critical errors
^^^^^^^^^^^^^^^^^^^^^^^^

SmartPipeline distinguishes two kinds of stage errors, managed by
:class:`.ErrorManager`:

* :class:`.SoftError` (raised explicitly by a stage): logged and attached to
  the item via :meth:`.Item.soft_errors`. Only the stage that raised it is
  skipped; the item continues to the next stages. (In a :class:`.BatchStage`,
  raising mid-loop skips the rest of that batch for that stage.)
* :class:`.CriticalError` (raised explicitly, or wrapping any unhandled
  exception): logged and attached to the item via :meth:`.Item.critical_errors`.
  By default, the item skips all remaining stages and is still yielded at the
  end of the pipeline.

You can change how critical errors are treated on the error manager:

* :meth:`.ErrorManager.raise_on_critical_error` stops the pipeline and re-raises
  the exception to the caller.
* :meth:`.ErrorManager.no_skip_on_critical_error` keeps sending the item to
  subsequent stages, like a soft error.
* Subclass :class:`.ErrorManager` and override :meth:`.ErrorManager.handle` for
  custom reporting or alerting, then register it with
  :meth:`.Pipeline.set_error_manager`.

When raising :class:`.SoftError` or :class:`.CriticalError` from a caught
exception, use explicit chaining (``raise SoftError("...") from e``) so the
original cause stays accessible via :meth:`.Error.get_exception`.

Stage retries
^^^^^^^^^^^^^

Transient failures (such as network timeouts or rate limits) can be retried per
stage by passing ``retryable_errors``, ``max_retries``, and ``backoff`` (for
exponential backoff) to :meth:`.Pipeline.append`:

.. code-block:: python

    pipeline = (
        Pipeline()
        .set_source(number_items(10))
        .append(
            "fetcher",
            RemoteFetcher(),
            concurrency=4,
            retryable_errors=(TimeoutError, ConnectionError),
            max_retries=3,
            backoff=0.5,
        )
        .build()
    )

    for item in pipeline.run():
        if item.has_soft_errors():
            for err in item.soft_errors():
                print(err.get_stage(), err, err.get_exception())

If all retry attempts fail, a single :class:`.RetryError` (a subclass of
:class:`.SoftError`) is attached to the item and that stage is skipped. Because
retries re-run the stage on the same :class:`.Item` instance, retried stages
should be idempotent or mutate ``item.data`` only after the fallible operation
succeeds.

Typing the payload
------------------

:class:`.Item`, :class:`.Source`, :class:`.Stage`, :class:`.BatchStage`, and
:class:`.Pipeline` are generic over the payload type and checked statically by
``mypy``. Without a type parameter, the payload defaults to ``Dict[str, Any]``.
Pass a ``TypedDict``, dataclass, or any other type to enforce it across the
pipeline:

.. code-block:: python

    from typing import TypedDict
    from smartpipeline import Item, Pipeline, Stage


    class Row(TypedDict):
        id: str
        text: str


    class UpperRow(Stage[Row]):
        def process(self, item: Item[Row]) -> Item[Row]:
            item.data["text"] = item.data["text"].upper()
            return item


    row_pipeline: Pipeline[Row] = (
        Pipeline[Row]()
        .set_source([Item[Row]({"id": "1", "text": "hello"})])
        .append("upper", UpperRow())
        .build()
    )

Complete example
----------------

The pipeline below reads HTML files from ``./document_files`` using the built-in
:class:`.LocalFilesSource` and :class:`.FilePathItem` helpers, extracts text,
finds VAT code occurrences, and indexes the documents in Elasticsearch. Errors
are logged to a dedicated Elasticsearch index through a custom
:class:`.ErrorManager`.

More self-contained runnable pipelines live in the ``examples/`` directory of
the repository.

.. code-block:: python

    import logging
    import re
    from typing import Optional

    from elasticsearch import Elasticsearch

    from smartpipeline import (
        CriticalError,
        ErrorManager,
        Item,
        Pipeline,
        SoftError,
        Stage,
    )
    from smartpipeline.helpers import FilePathItem, LocalFilesSource
    from smartpipeline.stage import AliveMixin


    class ESErrorLogger(ErrorManager):
        """Write stage error details into an Elasticsearch index."""

        def __init__(self, es_host: str, es_index: str):
            self.es_host = es_host
            self.es_index = es_index
            self.es_client = None

        def on_start(self):
            self.es_client = Elasticsearch(self.es_host)

        def on_end(self):
            if self.es_client is not None:
                self.es_client.close()

        def handle(
            self, error: Exception, stage: AliveMixin, item: Item
        ) -> Optional[CriticalError]:
            cause = error.get_exception() if isinstance(error, SoftError) else None
            logged_error = cause if cause is not None else error
            self.es_client.index(
                index=self.es_index,
                body={
                    "stage": str(stage),
                    "item": str(item),
                    "exception": str(type(logged_error)),
                    "message": str(logged_error),
                },
            )
            return super().handle(error, stage, item)


    class TextExtractor(Stage):
        """Read the text content of files."""

        def process(self, item: FilePathItem) -> Item:
            try:
                with open(item.path) as f:
                    item.data["text"] = f.read()
            except IOError as e:
                raise SoftError(f"Problems in reading file {item.path}") from e
            return item


    class VatFinder(Stage):
        """Identify Italian VAT codes in texts."""

        def __init__(self):
            self.regex = re.compile(
                r"^[A-Za-z]{2,4}(?=.{2,12}$)[-_\s0-9]*(?:[a-zA-Z][-_\s0-9]*){0,2}$"
            )

        def process(self, item: Item) -> Item:
            item.data["vat_codes"] = [
                (m.start(), m.end())
                for m in self.regex.finditer(item.data.get("text", ""))
            ]
            return item


    class Indexer(Stage):
        """Write item payloads into an Elasticsearch index."""

        def __init__(self, es_host: str, es_index: str):
            self.es_host = es_host
            self.es_index = es_index
            self.es_client = None

        def on_start(self):
            self.es_client = Elasticsearch(self.es_host)

        def on_end(self):
            if self.es_client is not None:
                self.es_client.close()

        def process(self, item: Item) -> Item:
            self.es_client.index(index=self.es_index, body=item.data)
            return item


    pipeline = (
        Pipeline()
        .set_error_manager(
            ESErrorLogger(
                es_host="localhost:9200", es_index="error_logs"
            ).raise_on_critical_error()
        )
        .set_source(LocalFilesSource("./document_files", postfix=".html"))
        .append("text_extractor", TextExtractor(), concurrency=2)
        .append("vat_finder", VatFinder())
        .append("indexer", Indexer(es_host="localhost:9200", es_index="documents"))
        .build()
    )

    for item in pipeline.run():
        logging.info("Processed document: %s", item)
