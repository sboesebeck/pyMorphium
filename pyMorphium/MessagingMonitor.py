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
    try:
        opts, args = getopt.getopt(argv,"?h:d:c:p",["host=","database=","collection="])
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

    try:
        with col.watch([{'$match': {'ns.db': dbname,'ns.coll':collection}}],'updateLookup') as stream:
            for insert_change in stream:
                if (insert_change['operationType']=='insert'):
                    fd=insert_change['fullDocument']
                    print("new message: ", fd['name'],fd['_id'])
                    print("sender     : ", fd['sender'])
                    if (fd['locked_by']=='ALL'):
                        print("Message is not exclusive")
                    elif ('recipient' in fd):
                        print("recipient  : ", fd['recipient'])
                    if ('in_answer_to' in fd):
                        print("anser to : ",fd['in_answer_to'])
                elif (insert_change['operationType']=='update'):
                    upd=insert_change['updateDescription']['updatedFields']
                    ks=upd.keys()
                    for k in ks:
                        if (str(k).startswith("processed_by")):
                            print("Message was processed by "+upd[k])
                            break
                print(insert_change)
                print("")
    except pymongo.errors.PyMongoError as e:
        # The ChangeStream encountered an unrecoverable error or the
        # resume attempt failed to recreate the cursor.
        logging.error('error during processing',e)

if __name__ == "__main__":
    main(sys.argv[1:])