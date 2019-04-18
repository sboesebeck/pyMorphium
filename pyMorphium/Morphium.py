import json

import jsonpickle
import pymongo

import mconfig


class Morphium:
    def __init__(self, config):
        print("morphium constructor")
        if isinstance(config, mconfig.Config):
            print("config ok")
        else:
            raise Exception("not of type config")
        if config.replicaset is not None:
            self.__client=pymongo.MongoClient(config.host_seed,config.replicaset)
        else:
            self.__client=pymongo.MongoClient(config.host_seed)

        self.database=self.__client[config.database]

    def save(self,obj):
        print("Saving instance")
        js=jsonpickle.encode(obj)
        print("JS is ", type(js))
        dec=json.JSONDecoder()
        m=dec.decode(js)
        print("m.value=", m["value"])
        print(type(m))
        col = self.database["testcol"]
        col.insert_one(m)

    def watch(self, listener):
        with self.__client.watch([{'$match': {'operationType': 'insert'}}]) as stream:
            for change in stream:
                print(change)
                listener.incomingChange(change)




    

class Query:
    def __init__(self):
        print("Create query")
