import os,sys,datetime,time
import MySQLdb

from hive_service import ThriftHive
from hive_service.ttypes import HiveServerException
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol


def loadData():
	initList = []
	
	initList.append("set mapred.job.queue.name=base") 
	initList.append("add jar /data/soft/hive/lib/hive-contrib-0.10.0-cdh4.2.0.jar")    
	initList.append("add jar /root/xiaofei/hive/hive_interval.jar")    
	initList.append("create temporary function get_elapse_interval as 'com.autonavi.hive.UDFInterval'")    
	initList.append("create temporary function get_distance_interval as 'com.autonavi.hive.UDFDistanceInterval'")    
	
	initList.append("LOAD DATA LOCAL INPATH '/data5/navi/car/car_schema_%s.dat' INTO TABLE navi.nvschema"%datestamp)
	initList.append("LOAD DATA LOCAL INPATH '/data5/navi/bus/bus_schema_%s.dat' INTO TABLE navi.nvschema"%datestamp)
	initList.append("LOAD DATA LOCAL INPATH '/data5/navi/snowman/snowman_schema_%s.dat' INTO TABLE navi.nvschema"%datestamp)

	try:
		for hiveql in initList:
			client.execute(hiveql)
	except Exception,e:
		print e

def executeMysql(sqlList):
    
    conn=MySQLdb.connect(host='*******',user='****',passwd='****',db='****',port=3306,charset='utf8')
    cur = conn.cursor()
    for sql in sqlList:
        # print>>writefile, sql
        cur.execute(sql)
    conn.commit()

    cur.close() 
    conn.close() 

def getime():
    datestamp =  (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    mytime = time.strftime('%H:%M:%S',time.localtime(time.time()))
    fintime = datestamp + ' ' + mytime
    return str(datetime.datetime.fromtimestamp(time.mktime(time.strptime(fintime, '%Y-%m-%d %H:%M:%S'))))
    

def hiveExec(sql,client):
    result = []
    try:
        client.execute(sql)
        while (1):
            row = client.fetchOne()
            if (row == None):
                break
            result.append(row)
    except Exception,e:
        print e

    return result

def getBasic():

	#calculate no. doesn`t success
	hiveql = "select count(1),service,platform from navi.nvschema where elapse='' and today='%s' group by service,platform"%datestamp
	data = hiveExec(hiveql,client)
	failHash = {}
	for line in data:
		(nFail, service, platform) = line.split('\t')
		failHash[service+platform] = int(nFail)

	#calculate no. of success
	hiveql = "select count(1),sum(distance),sum(elapse),service,platform from navi.nvschema where elapse!='' and today='%s' group by service,platform"%datestamp
	data = hiveExec(hiveql,client)
	sqlList = []
	for line in data:
		(nSuc, sumdis, sumelapse, service, platform)= line.split('\t')
		t = getime()
		if failHash.has_key(service+platform):
			nReq = int(nSuc)+failHash[service+platform]
		else:
			nReq = int(nSuc)

		avgtime = float(sumelapse)/float(nSuc)
		sql= 'insert into nv_basic_info (fre_request,fre_success,max_elapse,min_elapse,avg_elapse,sum_len,service,platform,day_time) values (%s,%s,%s,%s,%s,%s,"%s","%s","%s");' % (nReq,nSuc,0,0,avgtime,float(sumdis),service,platform,t)
		sqlList.append(sql)

	executeMysql(sqlList)


def getConnectivity():
	hiveql = "select startProvince,endProvince, service, platform,sum(distance),count(1) from navi.nvschema where startProvince != endProvince and elapse!='' and startProvince!='' and endProvince!='' and today='%s' group by service,platform,startProvince,endProvince"%datestamp
	print hiveql
	data = hiveExec(hiveql,client)
	sqlList = []
	for line in data:
		# print>>writefile, line
		(startProvince, endProvince, service, platform, sumdis, frequency)= line.split('\t')	
		t = getime()
		sql = 'insert into nv_connectivity (start_adcode,end_adcode,frequency,sum_len,service,platform,day_time) values (%s,%s,%s,%s,"%s","%s","%s");' % (startProvince,endProvince,frequency,sumdis,service,platform,t)
		sqlList.append(sql)

	executeMysql(sqlList)

def getLocationSum():
	hiveql = "select startProvince, service, platform, sum(distance), count(1) from navi.nvschema where startProvince !='' and elapse!='' and today='%s' group by service, platform, startProvince"%datestamp
	print hiveql
	data = hiveExec(hiveql,client)

	hiveql = "select startCity, service, platform, sum(distance), count(1) from navi.nvschema where startProvince !='' and elapse!='' and today='%s' group by service, platform, startCity"%datestamp
	print hiveql
	data2 = hiveExec(hiveql,client)

	mergeData = data + data2
	sqlList = []
	#### here we don`t care the calculation about end location point 
	#### so type = 0
	for line in mergeData:
		(adcode, service, platform, sumdis, frequency)= line.split('\t')
		if adcode !='':
			t = getime()
			sql = 'insert into nv_location_sum (adcode,name,frequency,day_time,service,platform,type,distance) values (%s,"%s",%s,"%s","%s","%s",%s,%s);' % (adcode,'',frequency,t,service,platform,0, sumdis)
			sqlList.append(sql)

	executeMysql(sqlList)


def getElapseInterval():
	hiveql = "select get_elapse_interval(elapse), count(1),service,platform from navi.nvschema where elapse!='' and today='%s' group by service, platform,get_elapse_interval(elapse)"%datestamp
	print hiveql
	data = hiveExec(hiveql,client)
	sqlList = []
	for line in data:
		(elapse_range, frequency, service, platform)= line.split('\t')
		t = getime()
		sql = 'insert into nv_elapse_interval (elapse_range,percentage,service,platform,day_time,elapsetype,frequency) values ("%s",%.6f,"%s","%s","%s",%s,%s);' % (elapse_range,0,service,platform,t,'0',frequency)
		sqlList.append(sql)

	executeMysql(sqlList)

def getDistanceInterval():
	#processing distance inteval first 
	hiveql = "select startProvince, get_distance_interval(distance),count(1),service,platform from navi.nvschema where elapse!='' and startProvince!='' and today='%s' group by service, platform,startProvince,get_distance_interval(distance)"%datestamp
	print hiveql
	data = hiveExec(hiveql,client)
	
	hiveql = "select startCity, get_distance_interval(distance),count(1),service,platform from navi.nvschema where elapse!='' and startCity!='' and today='%s' group by service, platform,startCity,get_distance_interval(distance)"%datestamp
	print hiveql
	data2 = hiveExec(hiveql,client)

	mergeData = data + data2

	sqlList = []

	for line in mergeData:
		(adcode, distance_range,frequency, service, platform)= line.split('\t')
		t = getime()
		sql = 'insert into nv_distance_interval (adcode, distance_range,percentage,service,platform,day_time,frequency) values ("%s","%s",%.6f,"%s","%s","%s",%s);' % (adcode,distance_range,0,service,platform,t,frequency)
		sqlList.append(sql)

	#processing distance inteval  sum then 
	hiveql = "select get_distance_interval(distance), count(1),service,platform  from navi.nvschema where elapse!='' and today='%s' group by service, platform,get_distance_interval(distance)"%datestamp
	print hiveql
	data = hiveExec(hiveql,client)


	for line in data:
		( distance_range,frequency, service, platform)= line.split('\t')
		t = getime()
		sql = 'insert into nv_distance_interval_sum (distance_range,percentage,service,platform,day_time,frequency) values ("%s",%.6f,"%s","%s","%s",%s);' % (distance_range,0,service,platform,t,frequency)
		sqlList.append(sql)

	executeMysql(sqlList)

if __name__ == '__main__':
	
	startTime = time.clock()
	writefile = open('debug.txt','w+')
	try:
		socket = TSocket.TSocket('localhost', 10000)
		transport = TTransport.TBufferedTransport(socket)
		protocol = TBinaryProtocol.TBinaryProtocol(transport)
		client = ThriftHive.Client(protocol)

		datestamp =  (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
		
		transport.open()
		client.execute('set mapred.job.queue.name=base')

		functions = (loadData,getBasic,getConnectivity,getLocationSum,getElapseInterval,getDistanceInterval)
		for f in functions:
			print f
			try:
				f()
			except Exception, tx:
				print tx

		transport.close()

	except Thrift.TException, tx:
		print'%s'%(tx.message)

	elapsed = (time.clock() - startTime)
	print ("time used: ", elapsed)