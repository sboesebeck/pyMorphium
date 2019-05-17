from tkinter import *
from pymongo import MongoClient
import _thread
import time
import pymongo
import getopt
import sys
 
#Config
host="msg1.genios.de"
port=27017
dbname="searchPermissions"
collection="msg"

additional=[ ]
types="anlpd"
useFilters=False
filters={}

messages={}
rowsToShow=20

argv=sys.argv[1:]

### reading in parameters
try:
    opts, args = getopt.getopt(argv,"?r:d:h:p:A:c:a:pt:",["port=","rows=","types=","additional=","host=","database=","collection=","filter="])
except getopt.GetoptError as e:
    print(e)
    print('TkMessagingMonitor.py -h|--host=<host> -d|database=<dbname> -c|collection=<collection> -a <ADDITIONAL Field> --filter=key:value --types=nlpda')
    sys.exit(2)
for opt, arg in opts:
    if opt == '-?':
        print('MessagingMonitor.py -h <host> -d <dbname> -c <collection> -p -a <ADDITIONAL Field> --filter=key:value --types=nlpda')
        sys.exit()
    elif opt in ("-r", "--rows"):
        rows=int(arg)
    elif opt in ("-p", "--port"):
        port=int(arg)
    elif opt in ("-h", "--host"):
        host = arg
    elif opt in ("-d", "--dbname"):
        dbname = arg
    elif opt in ("-c", "--collection"):
        collection = arg
    elif opt in ("-t", "--types"):
        types=arg
    elif opt in ("-a", "--additional"):
        additional.append(arg)
    elif opt == "--filter":
        flt=arg.split(":")
        filters[flt[0]]=flt[1]
        useFilters=True

columnCount=len(additional)+7

#build gui
root=Tk()
root.geometry("1900x1200+200+200")

Grid.rowconfigure(root,1,weight=0)
for y in range(rowsToShow+5):
    Grid.rowconfigure(root,y,weight=0)
for x in range(columnCount):
    Grid.columnconfigure(root,x,weight=1)

Grid.columnconfigure(root,5,weight=0)
Grid.columnconfigure(root,5,weight=2)

Label(root,text="").grid(row=0,column=0,columnspan=columnCount)
title=Label(root,text="Messaging Monitor: %s:%i %s/%s"%(host,port,dbname,collection),relief=RAISED,bg="#a0a0f0").grid(row=1,column=0,columnspan=columnCount,pady=5,sticky=W+E)

msgTotal=Label(root,text="Messages total: 0",bg="#f0f0f0")
msgTotal.grid(row=2,column=0,sticky=W+E)
msgUnprocessed=Label(root,text="Unprocessed messages: 0",bg="#f0f0f0")
msgUnprocessed.grid(row=2,column=1,sticky=W+E)
msgRate=Label(root,text="Message rate: 0 msg/sec",bg="#f0f0f0")
msgRate.grid(row=2,column=2,columnspan=1,sticky=W+E)

answersTotal=Label(root,text="Answers total: 0",bg="#f0f0f0")
answersTotal.grid(row=2,column=3,columnspan=1,sticky=W+E)
unansweredTotal=Label(root,text="Unanswered Msg: 0",bg="#f0f0f0")
unansweredTotal.grid(row=2,column=4,columnspan=1,sticky=W+E)
avgAnswerTimeL=Label(root,text="Average time for answer: 0ms",bg="#f0f0f0")
avgAnswerTimeL.grid(row=2, column=5,columnspan=1,sticky=W+E)

Label(root,text="Event",bg="#f0f0ff").grid(row=3,column=0,sticky=W+E)
Label(root,text="Name",bg="#f0f0ff").grid(row=3,column=1,sticky=W+E)
Label(root,text="msgId",bg="#f0f0ff").grid(row=3,column=2,sticky=W+E)
Label(root,text="sender",bg="#f0f0ff").grid(row=3,column=3,sticky=W+E)
Label(root,text="recipient",bg="#f0f0ff").grid(row=3,column=4,sticky=W+E)
Label(root,text="exclusive",bg="#f0f0ff").grid(row=3,column=5,sticky=W+E)
Label(root,text="in Answer to",bg="#f0f0ff").grid(row=3,column=6,sticky=W+E)
c=1
for i in additional:
    Label(root,text=i,bg="#f0f0ff").grid(row=3,column=6+c,sticky=W+E)
    c+=1

l=[]

for y in range(rowsToShow):
    l.append([])
    for x in range(columnCount):
       l[y].append(Label(root,text=""))
       l[y][x].grid(row=y+4,column=x,sticky=W+E)


#l[5][7].config(text="Hallo")
start=time.time()*1000
### messaging
messages={}

def msgLoop():
    client = MongoClient(host,port)
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
                    if (fd==None): 
                         msgId="none"
                         sender="none"
                         name="none" 
                         msg="none"
                         exclusive="none"
                         inAnswerTo=""
                    else:
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
                if (insert_change['operationType']=='insert'):
                    if 'n' not in types:
                       continue
                    msgType="new Message"
                    if inAnswerTo:
                        answers=answers+1
                        if inAnswerTo in messages:
                           dur=time.time()*1000-messages[inAnswerTo]
                           answerTimesSum+=dur
                    messages[msgId]=int(round(time.time()*1000))
                    num=num+1
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
                total=col.count_documents({})
                ans=col.count_documents({"in_answer_to":{"$ne":None}})
                unprocessed=col.count_documents({"processed_by":{"$size":1}})
                #print('Stats: Total Messages {}, answers {}, unprocessed {}'.format(total,ans,unprocessed))
                if answers==0:
                   avgAnswerTime=0
                else:
                   avgAnswerTime=answerTimesSum/answers
                #print('      answers registered {} - sum answering time {} - Avg. Answer time {} - {} unanswered messages'.format(answers,answerTimesSum,avgAnswerTime,len(messages)))
                msgTotal.config(text="Msg seen: "+str(num))
                avgAnswerTimeL.config(text="Avg Answer time: %.2f ms" % round(avgAnswerTime,2)) 
                answersTotal.config(text="Answert total: "+str(ans))
                dur=time.time()*1000-start
                rate=num/dur
                msgRate.config(text="MsgRate: %.2f/sec" % round(rate*1000,2))
                msgUnprocessed.config(text="Unprocessed: "+str(unprocessed))

                #print('{:>15} {:>5} - {:<50} {:<45} {:<25} {:<25} {:<38} {:<38} {:<6} {:<25} - {}'.format("time", "num#","msgType","name","msg","msgId","sender","recipient","excl","inAnswerTo",additional))
                 
                add=[]
                if fd!=None:
                   for k in additional:
                       if (k in fd):
                           add.append(fd[k])
                if inAnswerTo in messages:
                   dur=int(round(time.time()*1000))-messages[inAnswerTo]
                   messages.pop(inAnswerTo,None)
                   inAnswerTo=inAnswerTo+" after "+str(dur)+"ms"

                #print('{:>15} {:>5} - {:<50} {:<45} {:<25} {:<25} {:<38} {:<38} {:<6} {:<25} - {}'.format(int(round(time.time()*1000)), num,msgType,name,msg,msgId,sender,recipient,exclusive,inAnswerTo,add))
                for i in range(rowsToShow-1): 
                    for y in range(columnCount):
                        l[rowsToShow-2-i+1][y].config(text=l[rowsToShow-2-i][y].cget("text"))
                        l[rowsToShow-2-i+1][y].config(bg=l[rowsToShow-2-i][y].cget("bg"))
                        l[rowsToShow-2-i+1][y].config(fg=l[rowsToShow-2-i][y].cget("fg"))
                bg="#ffffff"
                if inAnswerTo!='none' and inAnswerTo!='':
                   bg='#e0f0e0'
                if num % 2 == 1:
                   bg="#e0e0e0"
                   if inAnswerTo!='none' and inAnswerTo!='': 
                      bg="#70e070"
                l[0][0].config(text=msgType,bg=bg)
                l[0][1].config(text=name,bg=bg)
                l[0][2].config(text=msgId,bg=bg)
                if msgId!='none' and msgId!='':
                   l[0][2].config(fg="#"+msgId[-6:])
                else:
                   l[0][2].config(fg="#000000")
                l[0][3].config(text=sender,bg=bg)
                l[0][4].config(text=recipient,bg=bg)
                l[0][5].config(text=str(exclusive),bg=bg)
                l[0][6].config(text=inAnswerTo,bg=bg)
                if fd != None and "in_answer_to" in fd: 
                   l[0][6].config(fg="#"+str(fd["in_answer_to"])[-6:])
                else: 
                   l[0][6].config(fg="#000000")
                c=0
                for i in additional:
                    if fd!=None:
                        if i in fd:
                           l[0][c+7].config(text=fd[i]) 
                        else:
                           l[0][c+7].config(text="")
                    l[0][c+7].config(bg=bg)
                    c=c+1
                root.update()
                #print(insert_change)
                #print("")

    except pymongo.errors.PyMongoError as e:
        # The ChangeStream encountered an unrecoverable error or the
        # resume attempt failed to recreate the cursor.
        logging.error('error during processing',e)

_thread.start_new_thread(msgLoop,())

mainloop()
