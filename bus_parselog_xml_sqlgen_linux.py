#coding=utf-8
import urllib2,sys, os,tarfile, re,datetime,json,socket,codecs,time, MySQLdb,math 
# from time import *

reload(sys)
sys.setdefaultencoding( "utf-8" )


rSuccess = re.compile(r'^NOTICE:\s(.*?)\s\[.*?\]\s\[tid:(\d+)\].*Successfully.*\[([\d|\.]+)\].*$')
rReceive = re.compile(r'^NOTICE:\s(.*?)\s\[.*?\]\s\[tid:(\d+)\].*receive:\[.*X1=(.+?)&Y1=(.+?)&X2=(.+?)&Y2=(.+?)&.*$')
reParseXML = re.compile(r'<province><name>(.*?)<\/name>.*?<code>(.*?)<\/code>.*?<city><name>(.*?)<\/name>.*?<code>(.*?)<\/code>.*?<district><name>(.*?)<\/name>.*?<code>(.*?)<\/code>',re.DOTALL)

today=''
writeError = open('error.txt','a')
########################################request information about start & end###########################################################
def searchinfo(x,y,todict,mytime,type):
    datestamp =  (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    writeLocation = open('nv_location_'+ datestamp +'.sql','a')
    logtime = getnow(mytime)
    sqlitem=[]
    
    website='http://192.168.1.218/sisserver.php?query_type=RGEOCODE&REGION&poinum=2&range=200&roadlevel=0&pattern=0&ignorePoi=0&query_src=test&user_info=test&qii=false&rgeo_server_ip=127.0.0.1&rgeo_server_port=13337&source_type=forNaviLog'
    #website='http://ylse.mapabc.com/sisserver.php?query_type=RGEOCODE&REGION&poinum=10&range=200&roadlevel=0&pattern=0&ignorePoi=0&query_src=test&user_info=test&qii=false&rgeo_server_ip=127.0.0.1&rgeo_server_port=13337'
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
        for item in sqlitem:
            #cur.execute('insert into nv_location (adcode,frequency,day_time,service,platform,type) values (%s,%s,%s,%s,%s,%s)',item)
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
def ParseRawLog(f,basicinfo,start,end):

    global today

    datestamp =  (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    writeCoordinates = open('nv_coordinates_'+ datestamp +'.sql','a')
    
    file = open(f)
    while 1:
        lines = file.readlines(100000)
        if not lines:
            break
        for line in lines:     
            match = rReceive.match(line.rstrip())
            matchSuc = rSuccess.match(line.rstrip())
            if match:
                #noReceive+=1
                today = match.group(1)
                tid = match.group(2)
                startx = match.group(3)
                starty = match.group(4)
                
                r=basicinfo.get('norec')
                basicinfo['norec']=r+1

                try:
                    searchinfo(startx,starty,start,today,'0')
                except:
                    print >> writeError, line
                    continue
                endx = match.group(5)
                endy = match.group(6)
                try:
                    searchinfo(endx,endy,end,today,'1')
                except:
                    print >> writeError, line
                    continue
                
                
                try:
                    dis = caldistant(startx,starty,endx,endy)
                    dislist = basicinfo.get('distant')
                    dislist.append(dis)
                except:
                    print >>  writeError, 'math domain error'
                    print >> writeError, line
                    continue
                

                ############################################insert nv_coordinates##################################################)
                value=[startx,starty,endx,endy,getime(today),logtype,'',dis]

                 
                #cur.execute('insert into nv_coordinates (slat,slng,dlat,dlng,time,service,platform,distance) values (%s,%s,%s,%s,%s,%s,%s,%s) ',value) 
                print>>writeCoordinates, 'insert into nv_coordinates (slat,slng,dlat,dlng,time,service,platform,distance) values (%s,%s,%s,%s,"%s","%s","%s",%s); ' % (startx,starty,endx,endy,getime(today),logtype,'',dis)
                #cur.execute('insert into nv_coordinates values(%s,%s,%s,%s,%s,%s,%s,%f)',value)   
                
            elif matchSuc:
                today = matchSuc.group(1)
                tid = matchSuc.group(2)
                elapse=matchSuc.group(3)
                eltime=float(elapse)
                
                insertdict(elapsedict,elapse)           #deal elapse time
                
                r=basicinfo.get('nosuc')
                basicinfo['nosuc']=r+1
                elist = basicinfo.get('elapse')
                elist.append(eltime)
                
        #print 'a while loop'
        #conn.commit()

###################################################insert nv_basic_info############################################################   
def insertbasic(basicinfo,today):

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
        sumdis = sumdis+item
    #insert database
    value = [noReceive,noSuccess,maxtime,mintime,avgtime,sumdis,logtype,'',getime(today)]
    #cur.execute('insert into nv_basic_info (fre_request,fre_success,max_elapse,min_elapse,avg_elapse,sum_len,service,platform,day_time) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)',value)
    print >>writeBasicInfo, 'insert into nv_basic_info (fre_request,fre_success,max_elapse,min_elapse,avg_elapse,sum_len,service,platform,day_time) values (%s,%s,%s,%s,%s,%s,"%s","%s","%s");' % (noReceive,noSuccess,maxtime,mintime,avgtime,sumdis,logtype,'',getime(today))

    #conn.commit()

#########################################insert nv_location_sum######################################################################
def locationsum(start,end,today):
    datestamp =  (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    writeLocationSum = open('nv_location_sum_'+ datestamp +'.sql','w+')

    for key in start:
        code = key.split(',')[0]
        name = key.split(',')[1]
        
        stavalue=[code,name,start[key],getime(today),logtype,'','0']
        #cur.execute('insert into nv_location_sum (adcode,name,frequency,day_time,service,platform,type) values (%s,%s,%s,%s,%s,%s,%s)',stavalue)
        print >> writeLocationSum,'insert into nv_location_sum (adcode,name,frequency,day_time,service,platform,type) values (%s,"%s",%s,"%s","%s","%s",%s);'  % (  code,name,start[key],getime(today),logtype,'','0')
    for key in end:
        code = key.split(',')[0]
        name = key.split(',')[1]
        endvalue=[code,name,end[key],getime(today),logtype,'','1']
        #print '########## ', 'insert into nv_location (adcode,frequency,day_time,service,platform,type) values (%s,%s,%s,%s,%s,%s)',endvalue
        #cur.execute('insert into nv_location (adcode,frequency,day_time,service,platform,type) values (%s,%s,%s,%s,%s,%s)',endvalue)
        sql = 'insert into nv_location_sum (adcode,name,frequency,day_time,service,platform,type) values (%s,"%s",%s,"%s","%s","%s",%s);' % (code,name,end[key],getime(today),logtype,'','1')
        #print sql
        print >> writeLocationSum, sql

    #conn.commit()
        
        
        
#######################################insert nv_elapse###############################################################################
def insertelapse(elapsedict,today):
    datestamp =  (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    writeElapse = open('nv_elapse_'+ datestamp +'.sql','w+')

    for key in elapsedict:
        value = [key,elapsedict[key],logtype,'',getime(today),'0']
        #cur.execute('insert into nv_elapse (elapse,frequency,service,platform,day_time,elapsetype) values (%s,%s,%s,%s,%s,%s)',value)
        sql = 'insert into nv_elapse (elapse,frequency,service,platform,day_time,elapsetype) values (%s,%s,"%s","%s","%s",%s);' % (key,elapsedict[key],logtype,'',getime(today),'0')
        #print sql
        print >>writeElapse,sql
        #conn.commit()
       

####################################main function########################################################################################

if __name__ == '__main__':
    #print 'please input the type of log you are dealing:'
    #logtype = raw_input()
    logtype = 'bus'
    '''
    conn=MySQLdb.connect(host='127.0.0.1',user='root',passwd='',db='navigation',port=3306,charset='utf8')
    cur = conn.cursor()
    '''

    datestamp =  (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    #log_dir = "nv_log"
    log_dir = "/root/xiaofei/navi/"+logtype+"/"+datestamp
    start={}
    end={}
    elapsedict={}
    
    basicinfo={'norec':0,'nosuc':0,'elapse':[],'distant':[] }
    
    
    for root, dirs, files in os.walk(log_dir):
        for name in files:
            print 'processing ',name
            ParseRawLog(os.path.join(root,name),basicinfo,start,end)
    

    #print 'today is : ++++ ' ,today
    #today = '02-18 22:49:56'
    locationsum(start,end,today)
    insertbasic(basicinfo,today)
    insertelapse(elapsedict,today)
            
    #cur.close()
    #conn.close()