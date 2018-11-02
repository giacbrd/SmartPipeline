import random
import socket
import time
from datetime import datetime
from time import sleep

from smartpipeline.error import Error, CriticalError
from smartpipeline.stage import Source, DataItem, Stage

__author__ = 'Giacomo Berardi <giacbrd.com>'

TEXT_SAMPLES = (
    'On the other hand, we denounce with righteous indignation and dislike men who are so beguiled and demoralized by the charms of pleasure of the moment,',
    'so blinded by desire, that they cannot foresee the pain and trouble that are bound to ensue; and equal blame belongs to those who fail in their duty through weakness of will,',
    'which is the same as saying through shrinking from toil and pain. These cases are perfectly simple and easy to distinguish.',
    'In a free hour, when our power of choice is untrammelled and when nothing prevents our being able to do what we like best, every pleasure is to be welcomed and every pain avoided.',
    'But in certain circumstances and owing to the claims of duty or the obligations of business it will frequently occur that pleasures have to be repudiated and annoyances accepted.',
    'The wise man therefore always holds in these matters to this principle of selection: he rejects pleasures to secure other greater pleasures, or else he endures pains to avoid worse pains.'
)


def random_text():
    return random.choice(TEXT_SAMPLES)


class FakeSource(Source):
    def __init__(self, count):
        self.total = count
        self.counter = 0

    def pop(self):
        self.counter += 1
        if self.counter > self.total:
            return None
        item = DataItem()
        item.payload.update({'text': random_text(), 'count': self.counter})
        return item


class ListSource(Source):
    def __init__(self, items):
        self.items = iter(items)

    def pop(self):
        return next(self.items)


class TextReverser(Stage):
    def process(self, item: DataItem):
        item.payload['text'] = item.payload['text'][::-1]
        return item


class TextDuplicator(Stage):
    def process(self, item: DataItem):
        item.payload['text_' + str(random.randint(1, 1000))] = item.payload['text']
        return item


class ExceptionStage(Stage):
    def process(self, item: DataItem):
        time.sleep(0.5)
        raise Exception('test exception')


class ErrorStage(Stage):
    def process(self, item: DataItem):
        raise Error('test pipeline error')


class CriticalErrorStage(Stage):
    def process(self, item: DataItem):
        raise CriticalError('test pipeline critical error')


def wait_service(timeout, predicate, args):
    start = datetime.now()
    while not predicate(*args):
        sleep(1)
        if (datetime.now() - start).seconds > timeout:
            raise TimeoutError()


def is_open(host, port):
    # FIXME are we shure this works?
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((host, int(port)))
        s.shutdown(2)
        return True
    except:
        return False
