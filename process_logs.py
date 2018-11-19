from pymongo import MongoClient
client = MongoClient()
odb = client['test-database']
cl_list = odb.list_collection_names()

  # if "oit_trace" in cl_list:
    # odb.drop_collection('oit_trace')

import pymongo
cl = odb['oit_user_sessions_old']
from datetime import datetime
users = cl.distinct("user_id")
user_schedule = {}
activity = [0.0 for x in range(0,24)]
messages = open("logs.txt",'r')
print("got users")
from datetime import timedelta	
mac_messages={}
import json

def getKey(item):	
	return item[0]


def  merge(intervals):
	intervals = sorted(intervals,key=getKey) 
 	merged = []
        for interval in intervals:
            # if the list of merged intervals is empty or if the current
            # interval does not overlap with the previous, simply append it
            if not merged or merged[-1][1] < interval[0]:
	    #if not merged or ((interval[0].day==merged[-1][1].day and (merged[-1][1] + timedelta(minutes=15)) < interval[0]) or (merged[-1][1] < interval[0] and interval[0].day!=merged[-1][1].day)):
                merged.append(interval)
            else:
            # otherwise, there is overlap, so we merge the current and previous
            # intervals.
                merged[-1][1] = max(merged[-1][1], interval[1])

        return merged
	
for x in users:
	messages = cl.find({"user_id":x})
	p = []
	for x in messages:
		p.append(x)
	a = open("users/user_logs_"+x+".txt",'w')
	for m in p:
		obj={}
		a.write(m["message"])
		mac_id = m["mac_id"]
		dt = m["date_time"]
		date_time = datetime.strptime(dt,'%Y-%m-%d %H:%M:%S')
		if mac_messages.get(mac_id)==None:
			mac_messages[mac_id]={}
			mac_messages[mac_id]["start"]=None
			mac_messages[mac_id]["schedule"]=[]
		z = m["message"]
		start = mac_messages[mac_id].get("start")
		if start!= None and "deauth" in z.lower():
                                print "Found end"
				if date_time.day > start.day:
					temp = start
					temp = temp.replace(hour=23,minute=59,second=59)
					
                                	mac_messages[mac_id]["schedule"].append([start,temp])
					temp = date_time
					temp = temp.replace(hour=0,minute=0,second=1)
					mac_messages[mac_id]["schedule"].append([temp,date_time])
				else:
					mac_messages[mac_id]["schedule"].append([start,date_time])
                                mac_messages[mac_id]["start"]=None
                                
                if "user authentication successful" in z.lower() and start==None:
                                mac_messages[mac_id]["start"] = date_time
                                print "Found start"
		
	intervals = []
	for key, value in mac_messages.items():
        	for z in value["schedule"]:
			intervals.append(z)
	print len(intervals)
	intervals = merge(intervals)
	print(len(intervals))
	k= open("users/" + x+ "_user_sessions.csv",'w')
	k.write("start,end\n")
	a.close()
	for z in intervals:
		k.write(str(z[0].date()) + " " + str(z[0].time()) + "," + str(z[1].date()) + " " + str(z[1].time())+"\n")

	k.close()
                                
	print "done with user: "+ str(x) 	
				
