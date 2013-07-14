#coding=utf-8
import urllib2,sys, os, re,datetime,json,time, math 
import socket,struct,codecs
import threading

reload(sys)
sys.setdefaultencoding( "utf-8" )

rSuccess = re.compile(r'\[(.*?)\]\[(.*?)\].*\[RESPONSE:ROUTE_SUCCESS\]\[(\d+)\]\[(\d+)\]\[(\d+)\W')
rReqPOST = re.compile(r'\[(.*?)\]\[(.*?)\]\[<route\s+(.*?)\>\<startpoint\>\<x\>(.+?)\</x\>\<y\>(.+?)\</y\>.*\<endpoint\>\<x\>(.+?)\</x\>\<y\>(.+?)\</y\>.*\[POST\]\[(\d+)\]\[(\d+)\].*$')
reParseXML = re.compile(r'<province><name>(.*?)<\/name>.*?<code>(.*?)<\/code>.*?<city>.*?<name>(.*?)<\/name>.*?<code>(.*?)<\/code>.*?<district><name>(.*?)<\/name>.*?<code>(.*?)<\/code>')
rReqGET = re.compile(r'^\[(.*?)\]\[(.*?)\]\[route\?(.*?)\]\[GET\]\[(\d+)\]\[(\d+)\]')
today=''
writeError = open('error.txt','a')

class Download(threading.Thread):
    def __init__(self, lines, lock, threadName):
        '''
        @param readlines: 文件行readlines。
        @param lock: 琐对象。
        @param threadName: 线程名称。
        '''
        super(Download, self).__init__(name=threadName)
        self.lines = lines
        self.lock = lock
        
    def run(self):
        #self.lock.acquire()
        lines = self.lines
        sock = initsock("****", 13337)
        
        for line in lines:     
            
            matchPOST = rReqPOST.match(line.rstrip())
            matchGET = rReqGET.match(line.rstrip())
            matchSuc = rSuccess.match(line.rstrip())
            schemaStr = ''
            if matchPOST:
                try:
                    paraHash = {}
                    sourcePlat = 'snowman'
                    today = matchPOST.group(1)
                    rtime = matchPOST.group(2)
                    
                    sessionid_re = matchPOST.group(8)
                    rtstamp = matchPOST.group(9)
                    startx = matchPOST.group(4)
                    starty = matchPOST.group(5)
                    endx = matchPOST.group(6)
                    endy = matchPOST.group(7)
                    
                    xmlAttri = matchPOST.group(3)
                    a = xmlAttri.split(' ')
                    for it in a:
                        if it != '':
                            b = it.split('=')

                            value = b[1]
                            if value[0] =='"':
                                value=value[1:]
                                value=value[:-1]
                            paraHash[b[0].lower()] = value

                    if paraHash.has_key('source'):  #  <route Type="0" Flag="135224" Vers="2.1" Uuid="0"><startpoint><x>113.343056</x><y>27.861696</y></startpoint><startpoint><x>113.343033</x><y>27.861609</y></startpoint><startpoint><x>113.343025</x><y>27.861538</y></startpoint><endpoint><x>113.355217</x><y>27.930515</y></endpoint></route>][POST][63030348406606953][1370521924041][]
                        sourcePlat = paraHash['source']
                    else:
                        sourcePlat = 'snowman'      

                    if paraHash.has_key('type'):  #  <route Type="0" Flag="135224" Vers="2.1" Uuid="0"><startpoint><x>113.343056</x><y>27.861696</y></startpoint><startpoint><x>113.343033</x><y>27.861609</y></startpoint><startpoint><x>113.343025</x><y>27.861538</y></startpoint><endpoint><x>113.355217</x><y>27.930515</y></endpoint></route>][POST][63030348406606953][1370521924041][]
                        typeValue = int(paraHash['type'], 16)
                    else:
                        typeValue = 0
                
                except Exception, e:
                    print>>writeDebug, line
                    continue
                    
                today = today.replace(':','-')
                schemaStr = splitTag.join((today, rtime, logtype, sourcePlat,'POST', sessionid_re, startx, starty))
                
                schemaStr = splitTag.join((schemaStr,searchinfo(startx,starty,sock)))
                schemaStr = splitTag.join((schemaStr,endx,endy))
                schemaStr = splitTag.join((schemaStr,searchinfo(endx,endy,sock)))
                
                if schemaHash.has_key(sessionid_re):
                    print>>writeSchema, schemaHash[sessionid_re][0]+splitTag+splitTag+splitTag
                    schemaHash[sessionid_re] = (schemaStr, rtstamp, typeValue)
                else:
                    schemaHash[sessionid_re] = (schemaStr, rtstamp, typeValue)
            
            elif matchGET:
                try:
                    paraHash = {}
                    today = matchGET.group(1)
                    rtime = matchGET.group(2)
                    sessionid_re = matchGET.group(4)
                    rtstamp = matchGET.group(5)

                    keyValue = matchGET.group(3)
                    
                    a = keyValue.split('&')
                    for it in a:
                        if it != '':
                            b = it.split('=')
                            paraHash[b[0].lower()] = b[1]

        
                    startx = paraHash['x1']
                    starty = paraHash['y1']
                    #####ignore stability TEST from yao.yang 
                    if startx=='116.3' and starty == '39.8':
                        #print 'matched'
                        continue

                    endx = paraHash['x2']
                    endy = paraHash['y2']
                    #sourcePlat = paraHash['Source']==''?'snowman':paraHash['Source']
                    if not paraHash.has_key('source'):
                        sourcePlat = 'snowman'
                    else:
                        sourcePlat =  paraHash['source']
                except Exception, e:
                    print>>writeDebug, line
                    continue

                today = today.replace(':','-')
                schemaStr = splitTag.join((today, rtime, logtype, sourcePlat,'GET', sessionid_re, startx, starty))

                schemaStr = splitTag.join((schemaStr,searchinfo(startx,starty,sock)))
                schemaStr = splitTag.join((schemaStr,endx,endy))
                schemaStr = splitTag.join((schemaStr,searchinfo(endx,endy,sock)))
                
                if schemaHash.has_key(sessionid_re):
                    print>>writeSchema, schemaHash[sessionid_re][0]+splitTag+splitTag+splitTag
                    schemaHash[sessionid_re] = (schemaStr, rtstamp, typeValue)
                else:
                    schemaHash[sessionid_re] = (schemaStr, rtstamp, typeValue)

            elif matchSuc:
                today = matchSuc.group(1)
                stime = matchSuc.group(2)
                sessionid_su = matchSuc.group(3)
                ststamp = matchSuc.group(4)
                distance = matchSuc.group(5)

                if schemaHash.has_key(sessionid_su):
                    schemaStr = schemaHash[sessionid_su][0]
                    rtstamp = schemaHash[sessionid_su][1]
                    elapse = float(int(ststamp)-int(rtstamp))
                    #new added!
                    typeValue = schemaHash[sessionid_su][2]
                    schemaStr+=splitTag +str(elapse)+ splitTag + str(distance)+ splitTag + str(typeValue)
                    print>>writeSchema, schemaStr
                    
                    del schemaHash[sessionid_su]


########################################request information about start & end###########################################################
def searchinfo(x,y,sock):
    ########### socket 
    query = "query_type:rgeocode\nx:"+str(x)+"\ny:"+str(y)+"\npoinum:3\nrange:200\nroadlevel:0\npattern:0\nignorePoi:0\nquery_src:test\nuser_info:test"
    result = connect(query, sock)
    myinfo = result.replace('\n', '')
    match = reParseXML.search(myinfo)
    locationStr = ''
    if match:
        procode = match.group(2)
        citycode = match.group(4)
        discode = match.group(6)
        
        if procode !='':
            #cause join , here do not need add one splitTag
            locationStr += procode
        # else: #cause join , here do not need add one splitTag
        #     locationStr += splitTag 

        if citycode!='':
            locationStr += splitTag
            locationStr += citycode
        else:
            locationStr += splitTag 

        if discode !='':
            locationStr += splitTag
            locationStr += discode
        else:
            locationStr += splitTag 
    return locationStr

#######################################format time################################################################################
def getime():
    
    datestamp =  (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    mytime = time.strftime('%H:%M:%S',time.localtime(time.time()))
    fintime = datestamp + ' ' + mytime
    return str(datetime.datetime.fromtimestamp(time.mktime(time.strptime(fintime, '%Y-%m-%d %H:%M:%S'))))
    
######################################################parse a log file###############################################################
def ParseRawLog(f):
    
    sessionid_re = ''
    sql = '0'
    datestamp =  (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    file = open(f)
    
    lock = threading.Lock()
    i = 1
    while 1:
        lines = file.readlines(100)
        if not lines:
            break
        thread_id = Download(lines, lock, "thread-" + str(i))
        thread_id.start()
        i = i+1
        thread_id.join()
        

############### socket connection  ###############
def initsock(host='127.0.0.1', port=13339):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        linger_struct=struct.pack('ii', 1, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, linger_struct)
        return sock
    except socket.error:
        pass
def connect(query, sock):
    try:
        head=0xffffeeee
        cmdtype=0
        length = len(query)+4
        q = struct.pack('III%ds'%len(query),  head,  length, cmdtype, query)
        sock.send(q)
        sock.recv(4)
        length,  = struct.unpack('I',  sock.recv(4))
        result = ''
        remain = length
        while remain:
            t = sock.recv(remain)
            result+=t
            remain-=len(t)
        result = result[:-1]
        return result
    except socket.error,e:
        print 'socket error:',e
        raise

####################################main function########################################################################################

if __name__ == '__main__':

    startTime = time.clock()

    logtype = 'car'
    splitTag = '@'

    schemaHash = {}
    
    log_dir = "****"
    #log_dir = "/data5/navi/refine/snowman"
    datestamp =  (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    writeSchema = open('snowman_schema_'+ datestamp +'.dat','a')
    writeDebug = open('fordebug.txt','a')
    for root, dirs, files in os.walk(log_dir):
        for name in files:
            if name.endswith(datestamp+'.log'):
                filePath = os.path.join(root, name)
                print 'processing ', filePath
                
                ParseRawLog(filePath)

    for it in schemaHash:
        schemaStr = schemaHash[it][0]
        schemaStr+= splitTag + splitTag+ splitTag
        print>>writeSchema, schemaStr
            
    elapsed = (time.clock() - startTime)
    print ("time used: ", elapsed)
