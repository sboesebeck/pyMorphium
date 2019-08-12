from pymongo import MongoClient
import pymongo
import getopt
import sys
import logging
import time



def main(argv):
    host = '127.0.0.1'
    dbname = 'test'
    collection = 'msg'
    usePolling=False
    useFilters=False
    additional=[]
    filters={}
    types="nlpda"
    stats=False
    modulo=10
    messages={}
    answerTimes=False
    try:
        opts, args = getopt.getopt(argv,"?sAh:m:d:c:a:pt:",["answertimes","stats","types=","additional=","host=","database=","collection=","filter="])
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
        elif opt in ("-t", "--types"):
            types=arg
        elif opt in ("-A", "--answerTimes"):
            answerTimes=True
        elif opt in ("-s", "--stats"):
            stats=True
        elif opt in ("-a", "--additional"):
            additional.append(arg)
        elif opt == "--filter":
            flt=arg.split(":")
            filters[flt[0]]=flt[1]
            useFilters=True
        elif opt == "-m":
            modulo=int(arg)
        elif opt == "-p":
            usePolling = True

    print('Host       :',host)
    print('dbname     :',dbname)
    print('collection :',collection)
    if (usePolling):
        print("using polling to get data.")
        print("not implemented yet, sorry")
        sys.exit(3)
    else:
        print("Watching for changes")
    client = MongoClient(host,27017)
    db=client[dbname]
    col=db[collection]
    num=0
    answers=0
    answerTimesSum=0
    try:
        with col.watch([{'$match': {'ns.db': dbname,'ns.coll':collection}}],'updateLookup') as stream:
            for insert_change in stream:
                name=""
                msgType=""
                msgId=""
                sender=""
                recipient=""
                exclusive=""
                inAnswerTo=""
                fd=None
                msg=""
                if ("fullDocument" in insert_change):
                    fd=insert_change['fullDocument']
                    if ( fd != None):
                        if ('_id' in fd):
                            msgId=str(fd['_id'])
                        sender=fd['sender']
                        name=fd['name']
                        if ('msg' in fd):
                           msg=fd['msg']
                        if ('recipient' in fd):
                           recipient=fd['recipient']
                        if ('in_answer_to' in fd):
                            if 'a' not in types:
                                continue
                            inAnswerTo=str(fd['in_answer_to'])
                        if (fd['locked_by']=='ALL'):
                            exclusive="false"
                        else:
                            exclusive="true"
                    else:
                        msgId="none"
                if (insert_change['operationType']=='insert'):
                    if 'n' not in types:
                       continue
                    msgType="new Message"
                    if inAnswerTo:
                        answers=answers+1 
                        if inAnswerTo in messages:
                           dur=time.time()*1000-messages[inAnswerTo]
                           answerTimesSum+=dur
                    elif answerTimes:
                        messages[msgId]=int(round(time.time()*1000))
                elif (insert_change['operationType']=='delete'):
                    if 'd' not in types:
                       continue
                    msgType="msg removed"
                elif (insert_change['operationType']=='update'):
                    msgType="msg update"
                    upd=insert_change['updateDescription']['updatedFields']
                    ks=upd.keys()
                    for k in ks:
                        if (str(k).startswith("processed_by") and 'p' in types):
                            msgType="processed by "+str(upd[k])
                            break
                        elif (str(k).startswith("locked_by") and 'l' in types):
                            msgType="locked by "+str(upd[k])
                            break
                    if msgType=="msg update":
                        continue
                if useFilters:
                    found=False
                    for k in filters:
                        if (k in fd):
                            if (fd[k] == filters[k]):
                                found=True
                        elif (k in insert_change):
                            if (insert_change[k]==filters[k]):
                                found=True
                    if not found:
                        continue
                num+=1
                if ((num%modulo) == 1):
                   print()
                   if stats:
                       total=col.count_documents({})
                       ans=col.count_documents({"in_answer_to":{"$ne":None}})
                       unprocessed=col.count_documents({"processed_by":{"$size":1}})
                       print('Stats: Total Messages {}, answers {}, unprocessed {}'.format(total,ans,unprocessed))
                       if answerTimes:
                           if answers==0: 
                               avgAnswerTime=0
                           else:
                               avgAnswerTime=answerTimesSum/answers
                           print('      answers registered {} - sum answering time {} - Avg. Answer time {} - {} unanswered messages'.format(answers,answerTimesSum,avgAnswerTime,len(messages)))
                   print('{:>15} {:>5} - {:<50} {:<45} {:<25} {:<25} {:<38} {:<38} {:<6} {:<25} - {}'.format("time", "num#","msgType","name","msg","msgId","sender","recipient","excl","inAnswerTo",additional))
                add=[]
                if fd!=None:
                   for k in additional:
                       if (k in fd):
                           add.append(fd[k])
                if answerTimes:
                   if inAnswerTo in messages:
                      dur=int(round(time.time()*1000))-messages[inAnswerTo]
                      messages.pop(inAnswerTo,None)
                      inAnswerTo=inAnswerTo+" after "+str(dur)+"ms"
                      
                print('{:>15} {:>5} - {:<50} {:<45} {:<25} {:<25} {:<38} {:<38} {:<6} {:<25} - {}'.format(int(round(time.time()*1000)), num,msgType,name,msg,msgId,sender,recipient,exclusive,inAnswerTo,add))
                #print(insert_change)
                #print("")

    except pymongo.errors.PyMongoError as e:
        # The ChangeStream encountered an unrecoverable error or the
        # resume attempt failed to recreate the cursor.
        logging.error('error during processing',e)

if __name__ == "__main__":
    main(sys.argv[1:])
