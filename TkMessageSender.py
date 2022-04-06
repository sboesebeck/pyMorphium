import sys

from pyMorphium import Morphium, Messaging


def main(args):
    cfg = Morphium.MConfig(host_seed=['localhost:27017'], database="test")
    cfg.replicaset = True
    cfg.database = "morphium_test"
    morphium = Morphium.Morphium(cfg)
    msg = Messaging.Messaging(morphium, cfg.database, "msg")
    msg.addListener(on_message)
    msg.start()


def on_message(msgType, name, msg, exclusive, msgId, sender, recipient, inAnswerTo, fd):
    print("message detected")

if __name__ == "__main__":
    main(sys.argv[1:])
