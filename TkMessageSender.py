from Morphium import Morphium
from Messaging import Messaging
from mconfig import Config
import sys



def main(args):
    cfg = Config(["localhost:27017"])
    cfg.replicaset = True
    cfg.database = "morphium_test"
    morphium = Morphium(cfg)
    msg=Messaging(morphium,cfg.database,"msg")
    msg.addListener(on_message)
    msg.start()


def on_message(msgType, name, msg, exclusive, msgId, sender, recipient, inAnswerTo, fd):
    print("message detected")

if __name__ == "__main__":
    main(sys.argv[1:])
