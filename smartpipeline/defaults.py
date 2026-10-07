__author__ = "Giacomo Berardi <giacbrd.com>"

CONCURRENCY_WAIT: float = 0.1  # seconds to wait in loops querying queues or threads
DATA_SNIPPET_SIZE: int = 100  # size of of the string defining a snippet of an item data
LOGS_STOP_TIMEOUT: float = 10.0  # seconds to wait for the logs receiver thread to end
MAX_QUEUES_SIZE: int = 1000  # default size for queues between concurrent stages, let's avoid infinite queues
