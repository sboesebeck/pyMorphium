import unittest

from pyMorphium import Morphium,Messaging


class TestMorphium(unittest.TestCase):
    def test_constructor(self):
        """
        Test constructor, should raise error, if cfg is not of type mconfig
        :return:
        """
        cfg = Morphium.MConfig(["localhost:27017"],"morphium_test")
        m = Morphium.Morphium(cfg)
        # all ok here
        cfg=[]
        exception=False
        try:
          m=Morphium.Morphium(cfg)
        except Exception:
            exception=True
        self.assertTrue(exception)

    def test_save(self):
        cfg = Morphium.MConfig(["localhost:27017"],"morphium_test")
        m = Morphium.Morphium(cfg)
        tst=TestEntity("test",5)
        m.save(tst)

class TestMessaging(unittest.TestCase):
    def test_Listening(self):
        cfg = Morphium.MConfig(["localhost:27017"],"morphium_test")
        m = Morphium.Morphium(cfg)
        messaging = Messaging.Messaging(m,"morphium_test","msg")
        print("Starting messaging")
        messaging.start()
        def incomingMessage(msgType, name, msg, exclusive, msgId, sender, recipient, inAnswerTo, fd):
            print("Incoming...")
        messaging.add_listener(incomingMessage)
        msg=Morphium.Msg(name="testmsg",value="value")
        messaging.send(msg)



class TestEntity:
    def __init__(self,value,num):
        self.value=value
        self.num=num

if __name__ == '__main__':
    unittest.main()
