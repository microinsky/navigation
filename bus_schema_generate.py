#coding=utf-8
import urllib2,sys, os,tarfile, re,datetime,json,time,math 
import socket,struct,codecs


reload(sys)
sys.setdefaultencoding( "utf-8" )


rSuccess = re.compile(r'^NOTICE:\s(.*?)\s\[.*?\]\s\[tid:(\d+)\].*Successfully.*\[([\d|\.]+)\].*$')
rReceive = re.compile(r'^NOTICE:\s(.*?)\s\[.*?\]\s\[tid:(\d+)\].*?receive:\[.*?(X1=.*?)\]')
reParseXML = re.compile(r'<province><name>(.*?)<\/name>.*?<code>(.*?)<\/code>.*?<city>.*?<name>(.*?)<\/name>.*?<code>(.*?)<\/code>.*?<district><name>(.*?)<\/name>.*?<code>(.*?)<\/code>',re.DOTALL)

today=''
writeError = open('error.txt','a')
########################################request information about start & end###########################################################
def searchinfo(x,y,sock):
    ########### socket 
    query = "query_type:rgeocode\nx:"+str(x)+"\ny:"+str(y)+"\npoinum:3\nrange:200\nroadlevel:0\npattern:0\nignorePoi:0\nquery_src:test\nuser_info:test"
    result = connect(query, sock)
    
    match = reParseXML.search(result)
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


##############################################calculate route distance############################################################    
class GPS:
    def deg2rad(self,d):
        return d*math.pi/180.0
    def spherical_distance(self,f, t):
        EARTH_RADIUS_METER =6378137.0;
        flon = self.deg2rad(f[1])
        flat = self.deg2rad(f[0])
        tlon = self.deg2rad(t[1])
        tlat = self.deg2rad(t[0])
        con = math.sin(flat)*math.sin(tlat)
        con += math.cos(flat)*math.cos(tlat)*math.cos(flon - tlon)
        return round(math.acos(con)*6378137.0,4)
        #return round(math.acos(con)*6378137.0/1000,4)
    
def caldistant(startx,starty,endx,endy):
    frompoint = [float(startx),float(starty)]
    topoint = [float(endx),float(endy)]
    g=GPS()
    return g.spherical_distance(frompoint,topoint)
#######################################format time################################################################################
def getime(mydate):
    
    myday = mydate.split(' ')[0]
    mytime = time.strftime('%H:%M:%S',time.localtime(time.time()))
    myear = time.strftime('%Y',time.localtime(time.time()))
    fintime = myear+'-'+myday+' '+mytime
    
    return str(datetime.datetime.fromtimestamp(time.mktime(time.strptime(fintime, '%Y-%m-%d %H:%M:%S'))))

def getnow(mydate):
    myear = time.strftime('%Y',time.localtime(time.time()))
    nowtime = myear+'-'+mydate
    return datetime.datetime.fromtimestamp(time.mktime(time.strptime(nowtime, '%Y-%m-%d %H:%M:%S')))
######################################################parse a log file###############################################################
def ParseRawLog(f):

    global today

    datestamp =  (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

    sock = initsock("****", 13337)
    
    file = open(f)
    while 1:
        lines = file.readlines(100000)
        if not lines:
            break
        for line in lines:     
            match = rReceive.match(line.rstrip())
            matchSuc = rSuccess.match(line.rstrip())
            if match:
                try:
                    paraHash = {}
                    today = match.group(1)
                    timestamp = str(getnow(today))
                    tid = match.group(2)
                    
                    keyValues = match.group(3)
                    
                    a = keyValues.split('&')
                    for it in a:
                        if it != '':
                            b = it.split('=')
                            paraHash[b[0].lower()] = b[1]
                    
                    if paraHash.has_key('source'): 
                        sourcePlat = paraHash['source']
                    else:
                        sourcePlat='unknown'

                    if paraHash.has_key('type') and paraHash['type']!='':  
                        typeValue = int(paraHash['type'], 16)
                    else:
                        typeValue = 0

                    startx = paraHash['x1']
                    starty = paraHash['y1']

                    endx = paraHash['x2']
                    endy = paraHash['y2']

                except:
                    print >>  writeError, 'paraHash error'
                    print >> writeError, line
                    continue

                schemaStr = splitTag.join((timestamp.split()[0], timestamp.split()[1], logtype, sourcePlat,'GET', tid, startx, starty))

                schemaStr = splitTag.join((schemaStr,searchinfo(startx,starty,sock)))
                schemaStr = splitTag.join((schemaStr,endx,endy))
                schemaStr = splitTag.join((schemaStr,searchinfo(endx,endy,sock)))


                try:
                    distance = int(caldistant(startx,starty,endx,endy))
                except:
                    print >>  writeError, 'math domain error'
                    print >> writeError, line
                    continue
                
                if schemaHash.has_key(tid):
                    print>>writeSchema, schemaHash[tid][0]+splitTag+splitTag+splitTag
                    schemaHash[tid] = (schemaStr, distance, typeValue)
                else:
                    schemaHash[tid] = (schemaStr, distance, typeValue)


            elif matchSuc:
                today = matchSuc.group(1)
                tid = matchSuc.group(2)
                elapse=matchSuc.group(3)
                eltime=float(elapse)
                distStr = ''
                schemaStr = ''
                
                if schemaHash.has_key(tid):

                    distance = schemaHash[tid][1]
                    typeValue = schemaHash[tid][2]
                    distStr = splitTag +str(eltime)+ splitTag + str(distance) + splitTag + str(typeValue)
                    schemaStr = schemaHash[tid][0]
                    schemaStr += distStr
                    print>>writeSchema, schemaStr
                    del schemaHash[tid]
                
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
    #print 'please input the type of log you are dealing:'
    #logtype = raw_input()
    logtype = 'bus'
    sourcePlat = 'unknown'
    splitTag = '@'

    schemaHash = {}
    log_dir = "****"
    #log_dir = "/data5/navi/refine/bus"
    datestamp =  (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    writeSchema = open('bus_schema_'+ datestamp +'.dat','a')
    for root, dirs, files in os.walk(log_dir):
        for name in files:
            if name.endswith(datestamp+'.log'):
                filePath = os.path.join(root, name)
                print 'processing ', filePath
                
                ParseRawLog(filePath)

    for it in schemaHash:
        schemaStr = schemaHash[it][0]
        schemaStr+= splitTag + splitTag + splitTag
        print>>writeSchema, schemaStr