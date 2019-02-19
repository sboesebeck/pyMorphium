import unittest

import morphium
from mconfig import Config

class TestMorphium(unittest.TestCase):
    def test_constructor(self):
        """
        Test constructor, should raise error, if cfg is not of type mconfig
        :return:
        """
        cfg=Config(["localhost:27017"])
        m=morphium.Morphium(cfg)
        # all ok here
        cfg=[]
        exception=False
        try:
          m=morphium.Morphium(cfg)
        except Exception:
            exception=True
        self.assertTrue(exception)

    def test_save(self):
        cfg=Config(["localhost:27017"])
        m=morphium.Morphium(cfg)
        tst=TestEntity("test",5)
        m.save(tst)


class TestEntity:
    def __init__(self,value,num):
        self.value=value
        self.num=num

if __name__ == '__main__':
    unittest.main()
