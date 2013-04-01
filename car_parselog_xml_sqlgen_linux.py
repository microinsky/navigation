#coding=utf-8
import urllib2,sys, os, re,datetime,json,socket,time, math 

reload(sys)
sys.setdefaultencoding( "utf-8" )

rSuccess = re.compile(r'\[(.*?)\]\[(.*?)\].*\[RESPONSE:ROUTE_SUCCESS\]\[(\d+)\]\[(\d+)\]\[(\d+)\].*$')
rReqPOST = re.compile(r'\[(.*?)\]\[(.*?)\].*\<startpoint\>\<x\>(.+?)\</x\>\<y\>(.+?)\</y\>.*\<endpoint\>\<x\>(.+?)\</x\>\<y\>(.+?)\</y\>.*\[POST\]\[(\d+)\]\[(\d+)\].*$')
reParseXML = re.compile(r'<province><name>(.*?)<\/name>.*?<code>(.*?)<\/code>.*?<city><name>(.*?)<\/name>.*?<code>(.*?)<\/code>.*?<district><name>(.*?)<\/name>.*?<code>(.*?)<\/code>',re.DOTALL)
rReqGET = re.compile(r'^\[(.*?)\]\[(.*?)\].*X1=(.+?)&Y1=(.+?)&X2=(.+?)&Y2=(.+?)&.*\[GET\]\[(\d+)\]\[(\d+)\].*$')

today=''
writeError = open('error.txt','a')

########################################request information about start & end###########################################################
def searchinfo(x,y,todict,mytime,type):
    datestamp =  (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    writeLocation = open('nv_location_'+ datestamp +'.sql','a')
    logtime = getnow(mytime)
    sqlitem=[]
    website='http://192.168.1.218/sisserver.php?query_type=RGEOCODE&REGION&poinum=2&range=200&roadlevel=0&pattern=0&ignorePoi=0&query_src=test&user_info=test&qii=false&rgeo_server_ip=127.0.0.1&rgeo_server_port=13337&source_type=forNaviLog'
    #website='http://211.151.71.27/sisserver.php?query_type=RGEOCODE&REGION&poinum=2&range=200&roadlevel=0&pattern=0&ignorePoi=0&query_src=test&user_info=test&qii=false&rgeo_server_ip=127.0.0.1&rgeo_server_port=13337&source_type=forNaviLog'
    #website='http://restapi.amap.com/rgeocode/simple?resType=json&encode=utf-8&range=1000&roadnum=2&crossnum=2&poinum=3&retvalue=1&sid=7001&region='
    location = 'x='+x+'&'+'y='+y
    url = website.replace('REGION', location)
    response = urllib2.urlopen(url)
    info = response.read()
    match = reParseXML.search(info)
    if match:
        proname = match.group(1)
        provinceName = proname.decode('gb2312').encode('utf-8')
        procode = match.group(2)
        provinceCode = procode.decode('gb2312').encode('utf-8')

        cityname = match.group(3)
        cityName = cityname.decode('gb2312').encode('utf-8')
        citycode = match.group(4)
        cityCode = citycode.decode('gb2312').encode('utf-8')
        

        disname = match.group(5)
        districtName = disname.decode('gb2312').encode('utf-8')
        discode = match.group(6)
        districtCode = discode.decode('gb2312').encode('utf-8')


        if provinceCode !=''and provinceName != '':
            proKey =provinceCode+','+provinceName
            insertdict(todict,proKey)
            proValue=[provinceCode,'1',logtime,logtype,'',type]
            sqlitem.append(proValue)
            
        if cityCode!=''and cityName!='':
            cityKey = cityCode +','+cityName
            insertdict(todict,cityKey)
            cityValue=[cityCode,'1',logtime,logtype,'',type]
            sqlitem.append(cityValue)
            
            
        if districtCode !=''and districtName !='':
            disKey = districtCode+','+districtName
            insertdict(todict,disKey)
            disValue=[districtCode,'1',logtime,logtype,'',type]
            sqlitem.append(disValue)
        #####################################insert nv_location##########################################################################
        #for item in sqlitem:
        #    cur.execute('insert into nv_location (adcode,frequency,day_time,service,platform,type) values (%s,%s,%s,%s,%s,%s)',item)
        for item in sqlitem:
            print>>writeLocation, 'insert into nv_location (adcode,frequency,day_time,service,platform,type) values (%s,%s,"%s","%s","%s",%s);' % (item[0], item[1],item[2],item[3],item[4],item[5])
            
def insertdict(mydict,mykey):
    
    if mydict.has_key(mykey):
        value = mydict.get(mykey)
        mydict[mykey] = value+1
    else:
        mydict[mykey]=1
    
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
        return round(math.acos(con)*6378137.0/1000,4)
    
def caldistant(startx,starty,endx,endy):
    frompoint = [float(startx),float(starty)]
    topoint = [float(endx),float(endy)]
    g=GPS()
    return g.spherical_distance(frompoint,topoint)
#######################################format time################################################################################
def getime():
    
    datestamp =  (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    mytime = time.strftime('%H:%M:%S',time.localtime(time.time()))
    fintime = datestamp + ' ' + mytime
    return str(datetime.datetime.fromtimestamp(time.mktime(time.strptime(fintime, '%Y-%m-%d %H:%M:%S'))))
    

def getnow(tstamp):
    mydate = float(tstamp)/1000
    datetime= time.localtime(mydate)
    dateStr= time.strftime('%Y-%m-%d %H:%M:%S',datetime)
    return dateStr

def printTime(msg):
    print msg,' >> ',time.time()

######################################################parse a log file###############################################################
def ParseRawLog(f,basicinfo,start,end):
    global today
    sessionid_re = ''
    sql = '0'
    datestamp =  (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    writeCoordinates = open('nv_coordinates_'+ datestamp +'.sql','a')
    
    file = open(f)
    while 1:
        lines = file.readlines(100000)
        if not lines:
            break
        for line in lines:     
            
            matchPOST = rReqPOST.match(line.rstrip())
            matchGET = rReqGET.match(line.rstrip())
            matchSuc = rSuccess.match(line.rstrip())
            #printTime('parse line') 
            if matchPOST:
                #printTime('match receive') 
                today = matchPOST.group(1)
                rtime = matchPOST.group(2)
                sessionid_re = matchPOST.group(7)
                rtstamp = matchPOST.group(8)
                startx = matchPOST.group(3)
                starty = matchPOST.group(4)
                #printTime('start searchinfo') 
                try:
                    searchinfo(startx,starty,start,rtstamp,'0')
                    
                except:
                    print >> writeError, line
                    continue
                
                endx = matchPOST.group(5)
                endy = matchPOST.group(6)
                try:
                    searchinfo(endx,endy,end,rtstamp,'1')
                    
                except:
                    print >> writeError, line
                    continue
                
                #printTime('end searchinfo') 

                if hashStack.has_key(sessionid_re):
                    if hashStack[sessionid_re] == 0:
                        hashStack[sessionid_re] = rtstamp
                else:
                    hashStack[sessionid_re]=rtstamp


                r=basicinfo.get('norec')
                basicinfo['norec']=r+1
                

               
                
                ############################################insert nv_coordinates##################################################)
                #value=[startx,starty,endx,endy,getnow(rtstamp),logtype,'',dis]
                #cur.execute('insert into nv_coordinates (slat,slng,dlat,dlng,time,service,platform,distance) values (%s,%s,%s,%s,%s,%s,%s,%s) ',value) 
                #print>>writeCoordinates, 'insert into nv_coordinates (slat,slng,dlat,dlng,time,service,platform,distance) values (%s,%s,%s,%s,"%s","%s","%s",%s); ' % (startx,starty,endx,endy,getnow(rtstamp),logtype,'',dis)
                hashStackSQL[str(sessionid_re)+'_sql'] = 'insert into nv_coordinates (slat,slng,dlat,dlng,time,service,platform,distance) values (%s,%s,%s,%s,"%s","%s","%s",%s); ' % (startx,starty,endx,endy,getnow(rtstamp),logtype,'','NEEDREPLACE')
            
            elif matchGET:
                #printTime('match receive') 
                
                

                today = matchGET.group(1)
                rtime = matchGET.group(2)
                sessionid_re = matchGET.group(7)
                rtstamp = matchGET.group(8)
                startx = matchGET.group(3)
                starty = matchGET.group(4)
                #printTime('start searchinfo') 
                try:
                    searchinfo(startx,starty,start,rtstamp,'0')
                    
                except:
                    print >> writeError, line
                    continue
                
                endx = matchGET.group(5)
                endy = matchGET.group(6)
                try:
                    searchinfo(endx,endy,end,rtstamp,'1')
                    
                except:
                    print >> writeError, line
                    continue
                
                #printTime('end searchinfo') 

                if hashStack.has_key(sessionid_re):
                    if hashStack[sessionid_re] == 0:
                        hashStack[sessionid_re] = rtstamp
                else:
                    hashStack[sessionid_re]=rtstamp


                r=basicinfo.get('norec')
                basicinfo['norec']=r+1
                

               
                
                ############################################insert nv_coordinates##################################################)
                #value=[startx,starty,endx,endy,getnow(rtstamp),logtype,'',dis]
                #cur.execute('insert into nv_coordinates (slat,slng,dlat,dlng,time,service,platform,distance) values (%s,%s,%s,%s,%s,%s,%s,%s) ',value) 
                #print>>writeCoordinates, 'insert into nv_coordinates (slat,slng,dlat,dlng,time,service,platform,distance) values (%s,%s,%s,%s,"%s","%s","%s",%s); ' % (startx,starty,endx,endy,getnow(rtstamp),logtype,'',dis)
                hashStackSQL[str(sessionid_re)+'_sql'] = 'insert into nv_coordinates (slat,slng,dlat,dlng,time,service,platform,distance) values (%s,%s,%s,%s,"%s","%s","%s",%s); ' % (startx,starty,endx,endy,getnow(rtstamp),logtype,'','NEEDREPLACE')
            
            elif matchSuc:
                today = matchSuc.group(1)
                stime = matchSuc.group(2)
                sessionid_su = matchSuc.group(3)
                ststamp = matchSuc.group(4)
                distant = matchSuc.group(5)
                
                r=basicinfo.get('nosuc')
                basicinfo['nosuc']=r+1
                

                dis = distant
                dislist = basicinfo.get('distant')
                dislist.append(dis)

                if hashStack.has_key(sessionid_su):               ##########in order to calculate elapse time
                    if not hashStack[sessionid_su] == 0:
                        rt = hashStack.get(sessionid_su)
                        elapse = int(ststamp)-int(rt)
                        insertdict(elapsedict,elapse)           #deal elapse time
                        eltime = float(elapse)
                        elist = basicinfo.get('elapse')
                        elist.append(eltime)
                        hashStack[sessionid_su] = 0
                

                    sql = hashStackSQL[str(sessionid_su)+'_sql']
                     
                    sql = sql.replace('NEEDREPLACE',dis)    
                    hashStackSQL[str(sessionid_su)+'_sql'] = '0' 
                    print>>writeCoordinates, sql
                
        #print len(lines)

        #print lines[0]
        #conn.commit()

###################################################insert nv_basic_info############################################################   
def insertbasic(basicinfo):
    datestamp =  (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    writeBasicInfo = open('nv_basic_'+ datestamp +'.sql','w+')
    noReceive = basicinfo.get('norec')
    noSuccess = basicinfo.get('nosuc')
    elist = basicinfo.get('elapse')
    dislist = basicinfo.get('distant')
    maxtime=0
    mintime=100000
    sumtime=0
    sumdis=0
    #deal with elapse time
    for item in elist:
        if item > maxtime:
            maxtime = item
        if item < mintime:
            mintime=item
        sumtime=sumtime+item
        
    print 'sum:  ',sumtime, ' noSucc: ', noSuccess
    avgtime = float(sumtime)/float(noSuccess)
    #deal with distant
    for item in dislist:
        sumdis = sumdis+int(item)
    #insert database
    #value = [noReceive,noSuccess,maxtime,mintime,avgtime,sumdis,logtype,'',getime()]
    t = getime()
    print >>writeBasicInfo, 'insert into nv_basic_info (fre_request,fre_success,max_elapse,min_elapse,avg_elapse,sum_len,service,platform,day_time) values (%s,%s,%s,%s,%s,%s,"%s","%s","%s");' % (noReceive,noSuccess,maxtime,mintime,avgtime,sumdis,logtype,'',t)
    #cur.execute('insert into nv_basic_info (fre_request,fre_success,max_elapse,min_elapse,avg_elapse,sum_len,service,platform,day_time) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)',value)
    #conn.commit()

#########################################insert nv_location_sum######################################################################
def locationsum(start,end):
    datestamp =  (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    writeLocationSum = open('nv_location_sum_'+ datestamp +'.sql','w+')
    
    for key in start:
        code = key.split(',')[0]
        name = key.split(',')[1]
        
        #stavalue=[code,name,start[key],getime(),logtype,'','0']
        t=getime()
        print >> writeLocationSum,'insert into nv_location_sum (adcode,name,frequency,day_time,service,platform,type) values (%s,"%s",%s,"%s","%s","%s",%s);'  % (  code,name,start[key],t,logtype,'','0')
        #cur.execute('insert into nv_location_sum (adcode,name,frequency,day_time,service,platform,type) values (%s,%s,%s,%s,%s,%s,%s)',stavalue)
    for key in end:
        code = key.split(',')[0]
        name = key.split(',')[1]
        #endvalue=[code,name,end[key],getime(),logtype,'','1']
        t=getime()
        sql = 'insert into nv_location_sum (adcode,name,frequency,day_time,service,platform,type) values (%s,"%s",%s,"%s","%s","%s",%s);' % (code,name,end[key],t,logtype,'','1')
        print >> writeLocationSum, sql
        #cur.execute('insert into nv_location_sum (adcode,name,frequency,day_time,service,platform,type) values (%s,%s,%s,%s,%s,%s,%s)',endvalue)
    #conn.commit()
        
        
        
#######################################insert nv_elapse###############################################################################
def insertelapse(elapsedict):
    datestamp =  (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    writeElapse = open('nv_elapse_'+ datestamp +'.sql','w+')
    for key in elapsedict:
        
        
        #value = [key,elapsedict[key],logtype,'',getime(),'0']
        t=getime()
        sql = 'insert into nv_elapse (elapse,frequency,service,platform,day_time,elapsetype) values (%s,%s,"%s","%s","%s",%s);' % (key,elapsedict[key],logtype,'',t,'0')
        print >>writeElapse,sql
       

####################################main function########################################################################################

if __name__ == '__main__':
    #print 'please input the type of log you are dealing:'

    start = time.clock()

    logtype = 'car'
    datestamp =  (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
    log_dir = "/root/xiaofei/navi/"+logtype+"/"+datestamp
    #log_dir = "test"
    
    '''
    conn=MySQLdb.connect(host='127.0.0.1',user='root',passwd='',db='navigation',port=3306,charset='utf8')
    cur = conn.cursor()
    '''
    
    start={}
    end={}
    elapsedict={}
    today=''
    basicinfo={'norec':0,'nosuc':0,'elapse':[],'distant':[] }
    hashStack={}
    hashStackSQL={}
    
    for root, dirs, files in os.walk(log_dir):
        for name in files:
            print 'processing ',name
            ParseRawLog(os.path.join(root,name),basicinfo,start,end)
            
            
            
            
    locationsum(start,end)
    insertbasic(basicinfo)
    insertelapse(elapsedict)
            
    elapsed = (time.clock() - start)
    print ("time used: ", elapsed)