from collections import OrderedDict as ODict

__author__ = 'Giacomo Berardi <giacbrd.com>'


class OrderedDict(ODict):

    def last_key(self):
        return next(reversed(self.keys()))

