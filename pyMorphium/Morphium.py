import json
import time
from dataclasses import dataclass,field

import bson
import jsonpickle
import pymongo



class Morphium:
    def __init__(self, config):
        print("morphium constructor")
        if isinstance(config, MConfig):
            print("config ok")
        else:
            raise Exception("not of type config")
        if config.replicaset is not None:
            self.__client = pymongo.MongoClient(config.host_seed, config.replicaset)
        else:
            self.__client = pymongo.MongoClient(config.host_seed)

        self.database = self.__client[config.database]

    def save(self, obj):
        print("Saving instance")
        js = jsonpickle.encode(obj)
        #print("JS is ", js)
        dec = json.JSONDecoder()
        m = dec.decode(js)
        print("m.value=", m["value"])
        colname=m["py/object"]
        colname=convertCamelCase(colname.removeprefix("pyMorphium.Morphium."))
        del(m["py/object"])
        doc={}
        for key, value in m.items():
            key=convertCamelCase(key)
            if (key == "id"):
                key="_id"
            doc[key]=value

        print(type(m))
        col = self.database[colname]
        col.insert_one(doc)
    # Driver code
    def watch(self, listener):
        with self.__client.watch([{'$match': {'operationType': 'insert'}}]) as stream:
            for change in stream:
                print(change)
                listener.incomingChange(change)

    def get_collection(self, dbname, colname):
        db = self.__client[dbname]
        return db[colname]


class Query:
    morphium: Morphium

    def __init__(self, m:Morphium):
        self.morphium = m
        print("Create query")


# current_milli_tim: () -> int = lambda: int(round(time.time() * 1000))

current_milli_time = lambda: int(round(time.time() * 1000))

@dataclass()
class Entity:
    id:bson.ObjectId=field(default=bson.ObjectId(),init=False)
@dataclass()
class Msg(Entity):
    name: str
    value: str
    timestamp: int = current_milli_time()
    lockedBy: str = "ALL"
    locked: int = 0
    ttl: int = 30000
    msg: str = None
    class_name: str = "de.caluga.morphium.messaging.Msg"
    sender: str = None
    processed_by: [] = None
    map_value: {} = None
    priority: int = 500
    sender_host: str = None
    delete_at: int = current_milli_time() + ttl

@dataclass
class MConfig:
    host_seed: []
    database: str
    replicaset: bool = False
    connection_timeout: int = 1000
    connections: int = 100



def convertCamelCase(string):
    res = [string[0].lower()]
    for c in string[1:]:
        if c in ('ABCDEFGHIJKLMNOPQRSTUVWXYZ'):
            res.append('_')
            res.append(c.lower())
        else:
            res.append(c)

    return ''.join(res)
