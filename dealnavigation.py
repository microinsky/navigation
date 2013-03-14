#coding=utf-8
import urllib2,sys, os,tarfile, re,datetime,json,socket,codecs,time, MySQLdb,math 
# from time import *

rSuccess = re.compile(r'^NOTICE:\s(.*?)\s\[.*?\]\s\[tid:(\d+)\].*Successfully.*\[([\d|\.]+)\].*$')
rReceive = re.compile(r'^NOTICE:\s(.*?)\s\[.*?\]\s\[tid:(\d+)\].*receive:\[.*X1=(.+?)&Y1=(.+?)&X2=(.+?)&Y2=(.+?)&.*$')

def searchinfo(x,y,todict,mytime,type):
    logtime = getnow(mytime)
    sqlitem=[]
    website='http://restapi.amap.com/rgeocode/simple?resType=json&encode=utf-8&range=1000&roadnum=2&crossnum=2&poinum=3&retvalue=1&sid=7001&region='
    url = website+x+','+y
    response = urllib2.urlopen(url)
    info = response.read()
    s = json.loads(info)
    status = s['status']
    if status != 'E0':
        print 'cannot find:'
        print x,y
    else:
        province = s['list'][0]['province']['name']
        procode = s['list'][0]['province']['code']
        provalue=[procode,'1',logtime,'bus','',type]
        sqlitem.append(provalue)
        city = s['list'][0]['city']['name']
        citycode = s['list'][0]['city']['code']
        district = s['list'][0]['district']['name']
        discode = s['list'][0]['district']['code']
        insertdict(todict,procode)
        if citycode!='':
            insertdict(todict,citycode)
            cityvalue=[citycode,'1',logtime,'bus','',type]
            sqlitem.append(provalue)
        insertdict(todict,discode)
        disvalue=[discode,'1',logtime,'bus','',type]
        sqlitem.append(disvalue)
        for item in sqlitem:
            cur.execute('insert into nv_location (adcode,frequency,day_time,service,platform,type) values (%s,%s,%s,%s,%s,%s)',item)
        
            

def insertdict(mydict,mykey):
    
    if mydict.has_key(mykey):
        value = mydict.get(mykey)
        mydict[mykey] = value+1
    else:
        mydict[mykey]=1
    
    
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

def ParseRawLog(f,cur):
    noReceive=0
    noSuccess = 0
    start={}
    end={}
    distant=[]
    elapsedict={}
    maxtime=0
    mintime=100000
    sumtime=0
    today=''
    file = open(f)
    while 1:
        lines = file.readlines(100000)
        if not lines:
            break
        for line in lines:     
            match = rReceive.match(line.rstrip())
            matchSuc = rSuccess.match(line.rstrip())
            if match:
                noReceive+=1
                today = match.group(1)
                tid = match.group(2)
                startx = match.group(3)
                starty = match.group(4)
                try:
                    searchinfo(startx,starty,start,today,'0')
                except:
                    continue
                endx = match.group(5)
                endy = match.group(6)
                try:
                    searchinfo(endx,endy,end,today,'1')
                except:
                    continue
                dis = caldistant(startx,starty,endx,endy)
                #print type(dis)
                distant.append(dis)
                dateTmp =  getime(today)
                value=[startx,starty,endx,endy, str(dateTmp),'bus','',dis]
                #for it in value:
                #    print it
                cur.execute('insert into nv_coordinates (slat,slng,dlat,dlng,time,service,platform,distance) values (%s,%s,%s,%s,%s,%s,%s,%s) ',value) 
                #print 'insert into nv_coordinates (slat,slng,dlat,dlng,time,service,platform,distance) values (%s,%s,%s,%s,%s,%s,%s,%s) ',value
                #cur.execute('insert into nv_coordinates values(%s,%s,%s,%s,%s,%s,%s,%f)',value)   
                
            elif matchSuc:
                noSuccess+=1
                today = matchSuc.group(1)
                tid = matchSuc.group(2)
                elapse=matchSuc.group(3)
                eltime=float(elapse)
                insertdict(elapsedict,eltime)
                #elapsetime.append(eltime)
                if eltime>maxtime:
                    maxtime=eltime
                if eltime<mintime:
                    mintime=eltime
                sumtime=sumtime+eltime
        print 'a while loop'
        conn.commit()
 
 
    
    for key in start:
        stavalue=[key,start[key],getime(today),'bus','','0']
        cur.execute('insert into nv_location_sum (adcode,frequency,day_time,service,platform,type) values (%s,%s,%s,%s,%s,%s)',stavalue)
    for key in end:
        endvalue=[key,end[key],getime(today),'bus','','1']
        #print 'insert into nv_location (adcode,frequency,day_time,service,platform,type) values (%s,%s,%s,%s,%s,%s)',endvalue
        cur.execute('insert into nv_location (adcode,frequency,day_time,service,platform,type) values (%s,%s,%s,%s,%s,%s)',endvalue)
 
                    
    avgtime = float(sumtime)/float(noSuccess)
    basicvalue = [noReceive,noSuccess,maxtime,mintime,avgtime,sumtime,'bus','',getime(today)]
    cur.execute('insert into nv_basic_info (fre_request,fre_success,max_elapse,min_elapse,avg_elapse,sum_len,service,platform,day_time) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)',basicvalue)
    
    for key in elapsedict:
        elvalue = [key,elapsedict[key],'bus','',getime(today),'']   
        #print  'insert into nv_elapse (elapse,frequency,service,platform,day_time,elapsetype) values (%s,%s,%s,%s,%s,%s)',stavalue   
        cur.execute('insert into nv_elapse (elapse,frequency,day_time,service,platform,elapsetype) values (%s,%s,%s,%s,%s,%s)',stavalue)
        
    conn.commit()
    
if __name__ == '__main__':

    start = time.clock()

        
    conn=MySQLdb.connect(host='127.0.0.1',user='root',passwd='',db='navigation',port=3306,charset='utf8')
    cur = conn.cursor()
    #f = 'route_city.txt'
    #ParseRawLog(f,cur)
    log_dir = "nv_log"
    for root, dirs, files in os.walk(log_dir):
        for name in files:
            ParseRawLog(os.path.join(root,name),cur)
    #cur.execute('create unique index user_uid_time_index on user(uid,time)')      
 
    cur.close()
    conn.close()

    elapsed = (time.clock() - start)
    print ("time used: ", elapsed)
