__author__ = "Giacomo Berardi <giacbrd.com>"

CONCURRENCY_WAIT: float = 0.1  # seconds to wait in loops querying queues or threads
PAYLOAD_SNIPPET_SIZE: int = 100  # size of of the string defining a snippet of an item payload
MAX_QUEUES_SIZE: int = 1000  # default size for queues between concurrent stages, let's avoid infinite queues
