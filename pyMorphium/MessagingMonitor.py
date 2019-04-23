from pymongo import MongoClient
import pymongo
import getopt
import sys
import logging



def main(argv):
    host = '127.0.0.1'
    dbname = 'test'
    collection = 'msg'
    usePolling=False
    useFilters=False
    filters={}
    try:
        opts, args = getopt.getopt(argv,"?h:d:c:p",["host=","database=","collection=,filter="])
    except getopt.GetoptError:
        print('MessagingMonitor.py -h <host> -d <dbname> -c <collection> -p')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-?':
            print('MessagingMonitor.py -h <host> -d <dbname> -c <collection> -p')
            sys.exit()
        elif opt in ("-h", "--host"):
            host = arg
        elif opt in ("-d", "--dbname"):
            dbname = arg
        elif opt in ("-c", "--collection"):
            collection = arg
        elif opt == "--filter":
            flt=arg.split(":")
            filters[flt[0]]=flt[1]
            useFilters=True
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

    try:
        with col.watch([{'$match': {'ns.db': dbname,'ns.coll':collection}}],'updateLookup') as stream:
            for insert_change in stream:
                num+=1
                name=""
                msgType=""
                msgId=""
                sender=""
                recipient=""
                exclusive=""
                inAnswerTo=""
                fd=None
                if ("fullDocument" in insert_change):
                    fd=insert_change['fullDocument']
                    msgId=str(fd['_id'])
                    sender=fd['sender']
                    name=fd['name']
                    if ('recipient' in fd):
                       recipient=fd['recipient']
                    if ('in_answer_to' in fd):
                        inAnswerTo=str(fd['in_answer_to'])
                    if (fd['locked_by']=='ALL'):
                        exclusive="not exclusive"
                    else:
                        exclusive="exclusive"

                if (insert_change['operationType']=='insert'):
                    msgType="new Message"
                elif (insert_change['operationType']=='delete'):
                    msgType="msg removed"
                elif (insert_change['operationType']=='update'):
                    msgType="msg update"
                    upd=insert_change['updateDescription']['updatedFields']
                    ks=upd.keys()
                    for k in ks:
                        if (str(k).startswith("processed_by")):
                            msgType="processed by "+upd[k]
                            break
                        elif (str(k).startWith("locked_by")):
                            msgType="locked by "+upd[k]
                            break
                if useFilters:
                    for k in filters:
                        if (k in fd):
                            if (fd[k] == filters[k]):
                                found=True
                        elif (k in insert_change):
                            if (insert_change[k]==filters[k]):
                                found=True
                    if not found:
                        continue
                print('{:<50} {:<35} {:<25} {:<38} {:<38} {:<10} {:<20}'.format("msgType","name","msgId","sender","recipient","exclusive","inAnswerTo"))
                print('{:<50} {:<35} {:<25} {:<38} {:<38} {:<10} {:<20}'.format(msgType,name,msgId,sender,recipient,exclusive,inAnswerTo))
                #print(insert_change)
                print("")

    except pymongo.errors.PyMongoError as e:
        # The ChangeStream encountered an unrecoverable error or the
        # resume attempt failed to recreate the cursor.
        logging.error('error during processing',e)

if __name__ == "__main__":
    main(sys.argv[1:])