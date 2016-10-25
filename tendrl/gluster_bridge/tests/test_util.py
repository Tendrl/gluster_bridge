import datetime
from tendrl.gluster_bridge import util


def test_now():
    assert type(util.now()) is datetime.datetime


class Test_util(object):

    def test_Ticker(self):
        self.ticker = util.Ticker(0, None)
        self.ticker._callback = self.ticker.stop
        assert self.ticker._complete.is_set() is False
        self.ticker._run()
        assert self.ticker._complete.is_set() is True

    def test_memoize(self):
        """testing memoize else block"""
        assert self.test_memoize_wrap() == "pytest"
        """testing memoize if block"""
        assert self.test_memoize_wrap() == "pytest"

    @util.memoize
    def test_memoize_wrap(self):
        return "pytest"
