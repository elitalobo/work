import gzip
import glob
import re
cnt=0
# from pymongo import MongoClient
from pymongo import MongoClient
import sys
import re
import glob
import gzip
from datetime import datetime
uMacs = {}
uNames = {}
messages = {}

fList = []
fList = glob.glob("*.gz")
fList.sort()
#fList.append("NET-wifi-all.log.20131128.encrypted.gz")

print str(len(fList))
#print str(fList)

client = MongoClient()
odb = client['test-database']
cl_list = odb.collection_names()

if len(cl_list) != 0:
  # if "oit_trace" in cl_list:
    # odb.drop_collection('oit_trace')

  if "oit_user_sessions" in cl_list:
    odb.drop_collection('oit_user_sessions')

cl = odb['oit_user_sessions']

kReg = re.compile(r"KERNEL")

regExUMac = re.compile(r".+>.+#(.+)#")
regExUName = re.compile(r".+>.+%(.+)%")
regExMsg = re.compile(r".+<(\d+)>")

fCount = 0
rtCount = 0
rfCount = 0
user_macs={}
def processLine(l,uMac,uName):	
	l = l.lower()
	if "user authentication successful" in l:
		k = l.split(" ")
		error_code = k[5][1:-1]
		message = l
		user_id =  uName
		mac_id = uMac
		user_macs[mac_id]=user_id
		m = l.split(" ")
                req = m[0].split(":")
                lent = len(req)-1
                dt = "2017 "+ req[lent] + " " + m[1] + " " + m[2]
                dt = datetime.strptime(str(dt),"%Y %b %d %H:%M:%S")
		date_time = dt.strftime('%Y-%m-%d %H:%M;%S')
		return {"message":message,"user_id":user_id,"mac_id":mac_id,"error_code":error_code,"date_time": date_time }
		
	elif "deauth" in l and user_macs.get(uMac)!=None:
		message = l
                user_id =  user_macs[uMac]
                mac_id = uMac
                m = l.split(" ")
                req = m[0].split(":")
                lent = len(req)-1
                dt = "2017 "+ req[lent] + " " + m[1] + " " + m[2]
                dt = datetime.strptime(str(dt),"%Y %b %d %H:%M:%S")
                date_time = dt.strftime('%Y-/%m-%d %H:%M;%S')
                return {"message":message,"user_id":user_id,"mac_id":mac_id,"error_code":error_code,"date_time": date_time }
	else:
		return None
import os

for f in os.listdir("logs"):
  fCount += 1
  print "file #: %i; %s" % (fCount, f)

  gFile = gzip.open("logs/" + f, 'rb')  
  
#  print "done with reading gzip"
  rfCount = 0
  
  for l in gFile:
    #import ipdb; ipdb.set_trace()
    rfCount += 1
    rtCount += 1

    if rfCount % 10000 == 0:
#      print l
      print "%i,%i,%i,%i,%i,%i" %(fCount, rtCount, rfCount, len(uMacs), len(uNames),len(messages))

    reUMac = regExUMac.search(l)
    reUName = regExUName.search(l)
    reMsg = regExMsg.search(l)
    uMac=None
    uName=None
    msg = None
    if reUMac != None:
      uMac = reUMac.group(1)
      
    
    if reUName != None:
      uName = reUName.group(1)
      

    if reMsg != None:
      msg = reMsg.group(1)

    l = l.lower()
	
    if "user authentication successful"  in l or "deauth" in l:
      obj = processLine(l,uMac,uName)
     

      try:
        if obj != None:
          id = cl.insert_one(obj).inserted_id
	  cnt+=1
	  if cnt%100000==0:
		print "inserted: " + str(cnt)
      except Exception as exp:
        print("-- ERROR: %s;\n  line: %s" % (str(exp), l))

        if client:
          client.close()
  
  gFile.close()


if client:
  client.close()
print str(uNames)
print str(uMacs)
print str(messages)
