from tkinter import *
import _thread
import time
import pymongo
import getopt
import sys
from pymongo import MongoClient
import random


#Config
host="msg1.genios.de"
port=27017
dbname="searchPermissions"
collection="msg"
width=800
height=600
additional=[ ]
useFilters=False
filters={}

messages={}

argv=sys.argv[1:]

### reading in parameters
try:
    opts, args = getopt.getopt(argv,"?d:h:p:A:c:a:pt:",["width=","height=","port=","additional=","host=","database=","collection=","filter="])
except getopt.GetoptError as e:
    print(e)
    print('TkMessagingMonitor.py -h|--host=<host> -d|database=<dbname> -c|collection=<collection> -a <ADDITIONAL Field> --filter=key:value ')
    sys.exit(2)
for opt, arg in opts:
    if opt == '-?':
        print('MessagingMonitor.py -h <host> -d <dbname> -c <collection> -p -a <ADDITIONAL Field> --filter=key:value')
        sys.exit()
    elif opt=='--width':
        width=int(arg)
    elif opt=='--height':
        height=int(arg)
    elif opt in ("-p", "--port"):
        port=int(arg)
    elif opt in ("-h", "--host"):
        host = arg
    elif opt in ("-d", "--dbname"):
        dbname = arg
    elif opt in ("-c", "--collection"):
        collection = arg
    elif opt in ("-a", "--additional"):
        additional.append(arg)
    elif opt == "--filter":
        flt=arg.split(":")
        filters[flt[0]]=flt[1]
        useFilters=True

###### Messaging

senders={}

toCreateBoxes=[]
toCreateLines=[]
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
                         if not sender in boxes:
                            toCreateBoxes.append(sender)
                            #b=c.create_oval(10,10+len(boxes)*25,50,50,fill="red")
                            #boxes[sender]=b;
                         name=fd['name']
                         if ('msg' in fd):
                            msg=fd['msg']
                         if ('recipient' in fd):
                            recipient=fd['recipient']
                            if not recipient in boxes and not recipient in toCreateBoxes: 
                                #b=c.create_oval(10,10+len(boxes)*25,50,50,fill="red")
                                #boxes[recipient]=b;
                                toCreateBoxes.append(recipient)
                            toCreateLines.append((sender,recipient))
                                #l=c.create_line(boxes[sender].coords()[0],boxes[sender].coords()[1],boxes[recipient].coords()[0],boxes[recipient].coords[1])
                                #lines[l]=0
                         else:
                            for i in boxes.keys():
                                if i != sender:
                                    toCreateLines.append((sender,i))
                         if ('in_answer_to' in fd):
                             inAnswerTo=str(fd['in_answer_to'])
                         if (fd['locked_by']=='ALL'):
                             exclusive="false"
                         else:
                             exclusive="true"
                if (insert_change['operationType']=='insert'):
                    print("incoming msg")
                    msgType="new Message"
                    if inAnswerTo:
                        answers=answers+1
                        if inAnswerTo in messages:
                           dur=time.time()*1000-messages[inAnswerTo]
                           answerTimesSum+=dur
                    messages[msgId]=int(round(time.time()*1000))
                    num=num+1
                elif (insert_change['operationType']=='delete'):
                       continue
                elif (insert_change['operationType']=='update'):
                    msgType="msg update"
                    upd=insert_change['updateDescription']['updatedFields']
                    ks=upd.keys()
                    for k in ks:
                        if (str(k).startswith("processed_by")):
                            msgType="processed by "+str(upd[k])
                            break
                        elif (str(k).startswith("locked_by")):
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
    except pymongo.errors.PyMongoError as e:
        # The ChangeStream encountered an unrecoverable error or the
        # resume attempt failed to recreate the cursor.
        logging.error('error during processing',e)



#### GUI Part

root=Tk()

c=Canvas(root)
c.pack()
lines={}
boxes={}
texts={}
#item[c.create_oval(50,50,100,100,fill="red")]=0
c.config(width=width,height=height)
def resize(event):
    print("Resizing %d x %d" % (event.width,event.height))
    w,h = event.width-6, event.height-6
    c.config(width=w, height=h)
    l=boxes.keys()
    for i in l:
        cnt=0
        x=0
        y=0
        while True:
            cnt=cnt+1
            x=random.randint(10,int(c.cget("width"))-230)
            y=random.randint(10,int(c.cget("height"))-50)
            fnd=False
            for b in l:
                if c.coords(boxes[b])[0]<x+230 and c.coords(boxes[b])[0]>x-230 and c.coords(boxes[b])[1]<y+50 and c.coords(boxes[b])[1]>y-50:
                    fnd=True
                    break
            if not fnd or cnt>100:
               break
        c.coords(boxes[i], (x,y,x+230,y+50))
        c.coords(texts[i], (x+105,y+25))
    for i in lines.keys():
       lines[i]=255

c.bind('<Configure>', resize)

movingBlock=None


def click(event):
    x,y=event.x,event.y
    ids=c.find_closest(x,y)
    print("Got ids" +str(ids))    
    c.coords(ids[0],(x,y,x+230,y+50))

 
    
c.bind("<B1-Motion>",click)
_thread.start_new_thread(msgLoop,())

last=time.time()*1000
while True:
    if time.time()*1000-last > 150:
       c.pack(fill="both", expand=True)
       last=time.time()
       #print("width: %d" % int(c.cget("width")))
       while len(toCreateBoxes)>0:
            cnt=0
            while True:
                cnt=cnt+1
                x=random.randint(10,int(c.cget("width"))-200)
                y=random.randint(10,int(c.cget("height"))-50)
                fnd=False
                for b in boxes.keys():
                    if c.coords(boxes[b])[0]<x+200 and c.coords(boxes[b])[0]>x-200 and c.coords(boxes[b])[1]<y+50 and c.coords(boxes[b])[1]>y-50:
                        fnd=True
                        break
                if not fnd or cnt>100:
                    break
            boxes[toCreateBoxes[0]]=c.create_oval(x,y,x+200,y+50,fill="red")
            texts[toCreateBoxes[0]]=c.create_text(x+105,y+25,text=toCreateBoxes[0],fill="white")
            del(toCreateBoxes[0])
       while len(toCreateLines)>0:
            l=c.create_line(c.coords(boxes[toCreateLines[0][0]])[0]+random.randint(0,200),c.coords(boxes[toCreateLines[0][0]])[1]+random.randint(0,50),c.coords(boxes[toCreateLines[0][1]])[0]+random.randint(0,200),c.coords(boxes[toCreateLines[0][1]])[1]+random.randint(0,50),fill="black",arrow="first")
            lines[l]=0
            del(toCreateLines[0])
       delete=[]
       for i in lines.keys():
           a=lines[i]
           a=a+2
           lines[i]=a
           if a >=255:
              delete.append(i)
           else: 
              col='{:x}'.format(a).zfill(2)
              col="#"+col+col+col
              c.itemconfig(i,fill=col)
       for i in delete:
           lines.pop(i,None)
           c.delete(i) 
    root.update_idletasks()
    root.update()
