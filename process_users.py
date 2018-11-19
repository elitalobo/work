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
print len(users)
for x in users:
	user_schedule[x]=[]
	user_activity = []	
	mac_messages = {}
	try:
		messages = cl.find({"user_id": x}).sort("date_time",pymongo.ASCENDING)
	except:
		print x
	for m in messages:
		mac_id = m["mac_id"]
		dt = m["date_time"]
		date_time = datetime.strptime(dt,'%Y-%m-%d %H:%M:%S')
		if mac_messages.get(mac_id)==None:
			mac_messages[mac_id]={}
			mac_messages[mac_id]["start"]=None
			mac_messages[mac_id]["schedule"]=[]
		z = m["message"]
		start = mac_messages.get("start")
		if start!= None and "deauth" in z.lower():
                                print "Found end"
                                mac_messages[mac_id]["schedule"].append((start,date_time))
                                mac_messages[mac_id]["start"]=None
                                
                if "user authentication successful" in z.lower():
                                mac_messages[mac_id]["start"] = date_time
                                #print "Found start"
                                
	print "done with user: "+ str(x) 	
				
