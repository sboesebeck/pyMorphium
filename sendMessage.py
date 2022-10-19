#!/usr/bin/env python3

import _thread
import getopt
import logging
import sys
import json
from datetime import datetime
from time import time, sleep

import pymongo
from bson.objectid import ObjectId
from pymongo.mongo_client import MongoClient
from pathlib import Path

# Defaults

global answers
answers = {}

global message
global quiet
global start
global delete
global checkFails
global checksDir

def main(argv):
    global message
    global quiet
    global answers
    global delete
    global client
    global db
    global col
    global checkFails
    global checksDir
    checksDir=""
    checkFails=0
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
    ttl=30
    wait = True
    listeners = []
    client = None
    db = None
    col = None
    quiet = False
    listeners = []
    minAnswers = 0
    maxAnswers = 0
    exclusive = False
    className="de.caluga.morphium.messaging.Msg"
    global check
    check=False
    global checks
    checks={}
    global checkDir
    global answers
    global start

    try:
        opts, args = getopt.getopt(argv, "?h:p:d:c:w:n:m:t:v:C:",
                                   ["port=", "map=", "class=","value=", "name=", "message=", "waittime=", "waitnum=", "host=","ttl=",
                                    "database=", "collection=", "dontwait", "quiet", "filter=", "minimum=", "maximum=", "exclusive",
                                    "checksFile=","check=","help","checksDir="])
    except getopt.GetoptError as e:
        print(e)
        help()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ('-?',"--help"):
            help()
            sys.exit()
        elif opt in ("-C", "--class"):
            className=arg
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
        elif opt == "--ttl":
            ttl == arg
        elif opt == "--exclusive":
            if wait:
                if awaitNum>1:
                    print("exclusive messages cannot get more than one answer")
                    awaitNum==1
            exclusive=True
        elif opt == "--dontwait":
            wait = False
        elif opt == "--quiet":
            quiet = True
        elif opt == "--minimum":
            minAnswers = int(arg)
        elif opt == "--maximum":
            maxAnswers = int(arg)
        elif opt == "--delete":
            delete = True
        elif opt == "--map":
            mv = arg.split(":")
            mapValue[mv[0]] = mv[1]
        elif opt == "--checksFile":
            file = open(arg, "r") 
            for line in file: 
                print("Got check line: ",)
                checks.append(line)
                check=True  
            close(file)
        elif opt== "--check":
            checks.append(arg)
            check=True
        elif opt == "--checksDir":
            checksDir=arg
            check=True 

    listeners.append(on_message)
    client = MongoClient(host, port)
    db = client[dbname]
    col = db[collection]

    # msgLoop()

    _thread.start_new_thread(msg_loop, (col, listeners, dbname, collection))
    global start
    start = time()


    if checksDir:
        print("Checking directory "+checksDir+" for checks")
        rootdir=Path(checksDir)
        file_list = [f for f in rootdir.glob('**/*') if f.is_file()]
        totalFails=0
        for f in file_list:
            print("Processing file..."+str(f))
            answers={}
            checkFails=0 
            content=f.read_text()
            jsonDefinition=json.loads(content)

            msg=jsonDefinition["message"]
            msg["_id"]=ObjectId()
            msg["processed_by"]=[]
            if "ttl" not in jsonDefinition or jsonDefinition["ttl"]=="ARG":
                msg["ttl"]=ttl
            else: 
                msg["ttl"]=int(jsonDefinition["ttl"])
            msg["delete_at"] = datetime.now().fromtimestamp(time() + msg["ttl"]/1000)
            msg["timestamp"] = int(time() * 1000)
            if jsonDefinition["exclusive"]=="False":
                msg["locked_by"]="ALL"
            else:
                msg["locked_by"]=None

            msg["processed_by"]=[]
            msg["sender"]="python_cmd"
            msg["sender_host"]="localhost"
            idx=1
            checks=jsonDefinition["checks"]
            col.insert_one(msg)
            print("Message sent")
            answers[msg["_id"]] = 0
            minimum=minAnswers
            endAfterMsgReceived=awaitNum
            if jsonDefinition["minAnswers"]!="ARG":
                minimum=int(jsonDefinition["minAnswers"])
            maximum=maxAnswers
            if jsonDefinition["maxAnswers"]!="ARG":
                maximum=int(jsonDefinition["maxAnswers"])
            if jsonDefinition["endAfterMsgReceived"]!="ARG":
                endAfterMsgReceived=int(jsonDefinition["endAfterMsgReceived"])
            print("Waiting for answers to message ",msg["_id"])
            print("     min: "+str(minimum)+" answers, max "+str(maximum)+" will quit after timout "+str(waitTime)+" or "+str(endAfterMsgReceived)+" answers")
            
            while True:
                if answers[msg["_id"]] >= endAfterMsgReceived or (time() - start) > waitTime:
                   break 
            if checkFails>0:
                print(str(checkFails)+" cheks in file "+str(f)+" failed!")    
                totalFails=totalFails+checkFails
            if minimum > 0 and answers[msg["_id"]] < minimum or 0 < maximum < answers[msg["_id"]]:
                print("ERROR: Number of answerwed messages is not between specified min and max")
                print("Received "+str(answers[msg["_id"]])+" min: "+str(minimum) + " max: "+str(maximum))
            print("... checks finished")
        print("All check files processed!")
        client.close()
        if (totalFails>0):
            print("ERROR: "+str(totalFails)+" tests failed")
            exit(1)
        else:
            exit(0)
                
    else:
        message = {}
        message["name"] = name
        message["_id"] = ObjectId()
        message["msg"] = msg
        message["value"] = value
        message["map_value"] = mapValue
        if exclusive:
            message["locked_by"] = None
        else:
            message["locked_by"] = "ALL"
        message["processed_by"] = []
        message["ttl"] = ttl*1000
        message["sender"] = "python_cmd"
        message["class_name"] = className
        message["timestamp"] = int(time() * 1000)
        message["delete_at"] = datetime.now().fromtimestamp(time() + ttl)

        col.insert_one(message)
        answers[message["_id"]] = 0
        print("Waiting for answer to message ",message["_id"])
        while wait:
            if answers[message["_id"]] >= awaitNum or (time() - start) > waitTime:
                break
        client.close()
        if not quiet:
            sleep(1.0)
            dur = time() - start
            print("%d Answers took %f sec" % (answers[message["_id"]], dur))
        if minAnswers > 0 and answers[message["_id"]] < minAnswers or 0 < maxAnswers < answers[message["_id"]]:
            print("ERROR: Number of answerwed messages is not between specified min and max")
            print("Received "+answers[message["_id"]]+" min: "+minAnswers + " max: "+maxAnswers)
            exit(1)
        if checkFails>0:
            print(str(checkFails)+" Checks failed!")
            exit(1)


def msg_loop(col, listeners, dbname, collection):
    global start
    global quiet
    global message 
    global checkFails
    print("Starting watch...")
    try:
        with col.watch([{'$match': {'ns.db': dbname, 'ns.coll': collection}}], 'updateLookup') as stream:
            for insert_change in stream:
                #logging.info("incoming change...")
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
                for listener in listeners:
                    listener(msgType, name, msg, exclusive, msgId, sender, recipient, inAnswerTo, fd)

    except pymongo.errors.PyMongoError as e:
        # The ChangeStream encountered an unrecoverable error or the
        # resume attempt failed to recreate the cursor.
        logging.error('error during processing', e)

def help():
    print('sendMessage.py OPTIONS')
    print(' Options are:')
    print('  -? Shows this little help')
    print('  -C|--class <NAME>: define the classname')
    print('  -h|--host <NAME>: define the mongo host')
    print('  -p|--port <NUM>: define the mongo port')
    print('  -d|--dbname <DB>: define the mongo database')
    print('  -c|--collection <name>: define the mongo collection')
    print('  -w|--waittime <secs>: number of seconds to wait')
    print('  -n|--waitnum <num>: number of answers to wait for. If the number is reached, program will exit')
    print('  -m|--name <name>: name of the message')
    print('  -t|--message <text>: value of the field msg')
    print('  -v|--value <value>: value of the field value')
    print('  --ttl <secs>: number of seconds after which this message should be deleted')
    print('  --exclusive: if set, send an exclusive message')
    print('  --dontwait: do not wait for answer')
    print('  --minimum <num>: minimum number of answers to be expected.')
    print('  --maximum <num>: maximum number of answers to be expected.')
    print('  --maximum <num>: maximum number of answers to be expected.')
    print('  --map FIELD:VALUE : define an entry for map value')
    print('  --check <CHECK> : run the check in python interpreter for each message, e.g. --check "msg[\'name\']==\'ping\'", can be added several times')
    print('  --checksFile <filename> : same as above, but all checks are stored in the file <filename>, one check per line')
        


def on_message(msgType, name, msg, exclusive, msgId, sender, recipient, inAnswerTo, fd):
    # print("in listener", msgType)
    global start
    global delete
    global col
    global checkFails
    if msgType == "new Message":
        if inAnswerTo is not None and inAnswerTo != '':
            if not quiet and ObjectId(inAnswerTo) in answers:
                print("Answer #%d to sent message after %f sec" % (answers[ObjectId(inAnswerTo)] + 1, (time() - start)))
                print("  name        : " + name)
                print("  msg         : " + msg)
                print("  sender      : " + sender)
                print("  msgID       : " + msgId)
                print("  recipient   : " + recipient)
                print("  sender_host : " + fd["sender_host"])
                print()
                answers[ObjectId(inAnswerTo)] = answers[ObjectId(inAnswerTo)] + 1
                if check:
                    print("Checking consistency...")
                    for line in checks:
                        try:
                            result=eval(line,{"msg":fd})
                            if result:
                                print("line >"+line+"< check ok!")
                            else:
                                print("ERROR: Check >"+line+"< of msg with _id=ObjectId('"+str(fd["_id"])+"') failed!")
                                checkFails=checkFails+1
                        except Exception as ex:
                            print(ex)
                            print("ERROR: check of line >"+line+"< failed with error, _id=ObjectId('"+str(fd["_id"])+"')!")
                            checkFails=checkFails+1

if __name__ == "__main__":
    main(sys.argv[1:])
