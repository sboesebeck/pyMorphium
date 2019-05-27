import _thread
import getopt
import logging
import sys
from datetime import datetime
from time import time

import pymongo
from bson.objectid import ObjectId
from pymongo import MongoClient

# Defaults

global answers
answers = {}

global message
global quiet
global start


def main(argv):
    global message
    global quiet
    msg = "ping"
    name = "ping"
    value = "value"
    mapValue = {}
    host = "localhost"
    port = 27017
    dbname = "morphium_test"
    collection = "msg"
    waitTime = 5
    awaitNum = 1
    wait = True
    listeners = []
    client = None
    db = None
    col = None
    quiet = False
    listeners = []
    global answers
    global start
    try:
        opts, args = getopt.getopt(argv, "?h:p:d:c:w:n:m:t:v:",
                                   ["port=", "map=", "value=", "name=", "message=", "waittime=", "waitnum=", "host=",
                                    "database=", "collection=", "dontwait", "quiet", "filter="])
    except getopt.GetoptError as e:
        print(e)
        print('MessagingMonitor.py -s|--stats -h|--host=<host> -d|database=<dbname> -c|collection=<collection> -p -a <ADDITIONAL Field> --filter=key:value --types=nlpda')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-?':
            print('MessagingMonitor.py -h <host> -d <dbname> -c <collection> -p -a <ADDITIONAL Field> --filter=key:value --types=nlpda')
            sys.exit()
        elif opt in ("-h", "--host"):
            host = arg
        elif opt in ("-d", "--dbname"):
            dbname = arg
        elif opt in ("-c", "--collection"):
            collection = arg
        elif opt in ("-w", "--waittime"):
            waitTime = int(arg)
        elif opt in ("-n", "--waitnum"):
            awaitNum = int(arg)
        elif opt in ("-m", "--name"):
            name = arg
        elif opt in ("-t", "--message"):
            msg = arg
        elif opt in ("-v", "--value"):
            value = arg
        elif opt in ("-p", "--port"):
            port = int(port)
        elif opt == "--dontwait":
            wait = False
        elif opt == "--quiet":
            quiet = True
        elif opt == "--map":
            mv = arg.split(":")
            mapValue[mv[0]] = mv[1]

    listeners.append(onMessage)
    client = MongoClient(host, port)
    db = client[dbname]
    col = db[collection]

    # msgLoop()

    _thread.start_new_thread(msgLoop, (col, listeners, dbname, collection))

    message = {}
    message["name"] = name
    message["_id"] = ObjectId()
    message["msg"] = msg
    message["value"] = value
    message["map_value"] = mapValue
    message["locked_by"] = "ALL"
    message["processed_by"] = []
    message["ttl"] = 30000
    message["sender"] = "python_cmd"
    message["class_name"] = "de.genios.jef.commands.ping.PingMsg"
    message["timestamp"] = int(time() * 1000)
    message["delete_at"] = datetime.now().fromtimestamp(time() + 5 * 60)

    global start
    start = time()
    col.insert_one(message)
    answers[message["_id"]] = 0
    # print("Waiting for answer to message ",message["_id"])
    while wait:
        if answers[message["_id"]] >= awaitNum or (time() - start) > waitTime:
            break
    client.close()
    if not quiet:
        dur = time() - start
        print("%d Answers took %f sec" % (answers[message["_id"]], dur))


def msgLoop(col, listeners, dbname, collection):
    global start
    global quiet
    global message
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
                if ("fullDocument" in insert_change):
                    fd = insert_change['fullDocument']
                    if fd == None:
                        continue
                    msgId = str(fd['_id'])
                    if "sender" in fd:
                        sender = fd['sender']
                    name = fd['name']
                    if ('msg' in fd):
                        msg = fd['msg']
                    if ('recipient' in fd):
                        recipient = fd['recipient']
                    if ('in_answer_to' in fd):
                        inAnswerTo = str(fd['in_answer_to'])
                    if ('locked_by' in fd and fd['locked_by'] == 'ALL'):
                        exclusive = "false"
                    else:
                        exclusive = "true"
                if (insert_change['operationType'] == 'insert'):
                    msgType = "new Message"
                elif (insert_change['operationType'] == 'delete'):
                    msgType = "msg removed"
                elif (insert_change['operationType'] == 'update'):
                    msgType = "msg update"
                    upd = insert_change['updateDescription']['updatedFields']
                    ks = upd.keys()
                    for k in ks:
                        if (str(k).startswith("processed_by")):
                            msgType = "processed by " + str(upd[k])
                            break
                        elif (str(k).startswith("locked_by")):
                            msgType = "locked by " + str(upd[k])
                            break
                if inAnswerTo == None:
                    continue
                for listener in listeners:
                    listener(msgType, name, msg, exclusive, msgId, sender, recipient, inAnswerTo, fd)

    except pymongo.errors.PyMongoError as e:
        # The ChangeStream encountered an unrecoverable error or the
        # resume attempt failed to recreate the cursor.
        logging.error('error during processing', e)


def onMessage(msgType, name, msg, exclusive, msgId, sender, recipient, inAnswerTo, fd):
    # print("in listener", msgType)
    global start
    if (msgType == "new Message"):
        if inAnswerTo != None and inAnswerTo != '':
            if ObjectId(inAnswerTo) in answers:
                print("Answer #%d to sent message after %f sec" % (answers[ObjectId(inAnswerTo)] + 1, (time() - start)))
                print("  name        : " + name)
                print("  msg         : " + msg)
                print("  sender      : " + sender)
                print("  msgID       : " + msgId)
                print("  recipient   : " + recipient)
                print("  sender_host : " + fd["sender_host"])
                print()
                answers[ObjectId(inAnswerTo)] = answers[ObjectId(inAnswerTo)] + 1


if __name__ == "__main__":
    main(sys.argv[1:])
