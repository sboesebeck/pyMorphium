import Morphium


class Messaging:
    def __init__(self, morphium):
        if isinstance(morphium, Morphium):
            print("morphium found")
        else:
            raise Exception("not of type config")
        self.listeners = []
        # change stream => Listener
        # todo background thread
        #

    def sendMessage(self, message):
        1

    def addListener(self, listener):
        self.listeners.append(listener)
