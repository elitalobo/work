from pymongo import MongoClient
client = MongoClient()
odb = client['test-database']
cl_list = odb.list_collection_names()
import pymongo
cl = odb['test_oit_user_sessions_old']

from datetime import datetime
users = cl.distinct("user_id")


import json
r = open("logs.txt",'w+')
messages = cl.find({"user_id": users[0]}).sort("date_time",pymongo.ASCENDING)
for p in messages:
     k = {}
     k["date_time"] = p["date_time"]
     k["mac_id"] = p["mac_id"]
     if "authentication successful" in  p["message"]:
             k["message"] = "authentication successful"
     else:
             k["message"] = "deauth"
     r.write(json.dumps(k)+"\n")
 
 

