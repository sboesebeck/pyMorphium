import _thread
import logging

import pymongo

from pyMorphium import Morphium


class Messaging:
    def __init__(self, morphium: Morphium.Morphium, dbname: str, collection: str):
        if isinstance(morphium, Morphium.Morphium):
            print("morphium first parameter needs to be a Morphium instance")
        else:
            raise Exception("not of type morphium")
        self.listeners = []
        self.__morphium = morphium
        self.dbname = dbname
        self.collection = collection
        # change stream => Listener
        # todo background thread
        #

    def sendMessage(self, message):
        1

    def addListener(self, listener):
        self.listeners.append(listener)

    def msg_loop(self, dbname, collection):
        col = self.__morphium.get_collection(dbname, collection)
        try:
            with col.watch([{'$match': {'ns.db': dbname, 'ns.coll': collection}}], 'updateLookup') as stream:
                for insert_change in stream:
                    # logging.info("incoming change...")
                    name = ""
                    msgType = ""
                    msgId = ""
                    sender = ""
                    recipient = ""
                    exclusive = ""
                    inAnswerTo = ""
                    fd = None
                    msg = ""
                    if "fullDocument" in insert_change:
                        fd = insert_change['fullDocument']
                        if fd is None:
                            continue
                        msgId = str(fd['_id'])
                        if "sender" in fd:
                            sender = fd['sender']
                        name = fd['name']
                        if 'msg' in fd:
                            msg = fd['msg']
                        if 'recipient' in fd:
                            recipient = fd['recipient']
                        if 'in_answer_to' in fd:
                            inAnswerTo = str(fd['in_answer_to'])
                        if 'locked_by' in fd and fd['locked_by'] == 'ALL':
                            exclusive = "false"
                        else:
                            exclusive = "true"
                    if insert_change['operationType'] == 'insert':
                        msgType = "new Message"
                    elif insert_change['operationType'] == 'delete':
                        msgType = "msg removed"
                    elif insert_change['operationType'] == 'update':
                        msgType = "msg update"
                        upd = insert_change['updateDescription']['updatedFields']
                        ks = upd.keys()
                        for k in ks:
                            if str(k).startswith("processed_by"):
                                msgType = "processed by " + str(upd[k])
                                break
                            elif str(k).startswith("locked_by"):
                                msgType = "locked by " + str(upd[k])
                                break
                    if inAnswerTo == None:
                        continue
                    for listener in self.listeners:
                        listener(msgType, name, msg, exclusive, msgId, sender, recipient, inAnswerTo, fd)

        except pymongo.mongo_client.PyMongoError as e:
            # The ChangeStream encountered an unrecoverable error or the
            # resume attempt failed to recreate the cursor.
            logging.error('error during processing', e)

    def start(self):
        _thread.start_new(self.msg_loop, (self.dbname, self.collection))
