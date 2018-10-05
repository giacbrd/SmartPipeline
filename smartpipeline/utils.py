from collections import OrderedDict as Dict

__author__ = 'Giacomo Berardi <giacbrd.com>'


class OrderedDict(Dict):

    def last_key(self):
        return next(reversed(self.keys()))
