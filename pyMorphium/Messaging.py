import Morphium


class Messaging:
    def __init__(self, morphium):
        if isinstance(morphium, Morphium):
            print("morphium found")
        else:
            raise Exception("not of type config")
