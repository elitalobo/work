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
#fList = glob.glob("logs/*.gz")
#fList.sort()
#fList.append("NET-wifi-all.log.20131128.encrypted.gz")

print (str(len(fList)))
#print str(fList)

client = MongoClient()
odb = client['test-database']
cl_list = odb.list_collection_names()

if len(cl_list) != 0:
  # if "oit_trace" in cl_list:
    # odb.drop_collection('oit_trace')

  if "oit_user_sessions" in cl_list:
    odb.drop_collection('test_oit_user_sessions_old')
    print "dropped"

cl = odb['test_oit_user_sessions_old']
kReg = re.compile(r"KERNEL")

regExUMac = re.compile(r".+>.+#(.+)#")
regExUName = re.compile(r".+>.+%(.+)%")
regExMsg = re.compile(r".+<(\d+)>")

fCount = 0
rtCount = 0
rfCount = 0
user_macs={}
def remove_white_spaces(m):
	temp = []
	for x in m:
		if x.strip()!="":
			temp.append(x.strip())
	return temp

found=0
kfound=0
afound=0
#umacs_found=set([])
#umacs_not_found = set([])
#umacs_not_found_count = {}
errors = open("errors.txt",'w+')
def processLine(l):	
	global errors
	global found
	global kfound
	global afound
	l = l.lower()
	k = l.split(" ") 
        error_code = k[5][1:-1]
	try:
		if "deauth from sta:" in l or "deauth to sta:" in l :
			afound+=1
		if "authentication successful" in l:
			message = l
			user_id =  l.split("username=")[1].split(" ")[0][1:-1]
			mac_id = l.split("mac=")[1].split(" ")[0][1:-1]
			#if mac_id in umacs_not_found:
			#	print "len: "+ str(len(umacs_not_found))
			#	umacs_not_found.remove(mac_id)
			#umacs_found.add(mac_id)
			user_macs[mac_id]=user_id
			m = l.split(" ")
			m = remove_white_spaces(m)
        	        req = m[0].split(":")
                	lent = len(req)-1
           	     	dt = "2017 "+ req[lent] + " " + m[1] + " " + m[2]
                	dt = datetime.strptime(str(dt),"%Y %b %d %H:%M:%S")
			date_time = dt.strftime('%Y-%m-%d %H:%M:%S')
			kfound+=1
			return {"message":message,"user_id":user_id,"mac_id":mac_id,"error_code":error_code,"date_time": date_time }
		elif "deauth from sta:" in l or "deauth to sta:" in l:
			a = l.split("deauth from sta:")
			if len(a)!=1:
				uMac = a[1].strip().split(" ")[0].strip()[1:-2]
			else:
				b = l.split("deauth to sta:")
				uMac = b[1].strip().split(" ")[0].strip()[1:-2]
			if user_macs.get(uMac)==None:
			#	if(uMac in umacs_not_found):
			#		umacs_not_found_count[uMac]+=1
			#		if umacs_not_found_count[uMac]>5:
			#			print uMac
			#			print l
			#	else:
	                #               umacs_not_found.add(uMac)
#
#					umacs_not_found_count[uMac]=1
				return None
			else:
				message = l
	                	user_id =  user_macs.get(uMac)
        	        	mac_id = uMac
                		m = l.split(" ")
				m = remove_white_spaces(m)
       	         		req = m[0].split(":")
                		lent = len(req)-1
                		dt = "2017 "+ req[lent] + " " + m[1] + " " + m[2]
                		dt = datetime.strptime(str(dt),"%Y %b %d %H:%M:%S")
                		date_time = dt.strftime('%Y-%m-%d %H:%M:%S')
				found+=1
				#print "found deauth: "+ str(found)
				#print "found auth: " + str(kfound)
				#print "found adeauth: " + str(afound)
                		return {"message":message,"user_id":user_id,"mac_id":mac_id,"error_code":error_code,"date_time": date_time }
		else:
			return None
	except:
		import ipdb; ipdb.set_trace()
		errors.write(l)
		return None
	
import os
entries = []
for f in os.listdir("logs1"):
  fCount += 1
  print "file #: %i; %s" % (fCount, f)
  #gFile = open("log.txt",'r')
  gFile = gzip.open("logs1/" + f, 'rb')  
  
#  print "done with reading gzip"
  rfCount = 0
  
  for l in gFile:
    #import ipdb; ipdb.set_trace()
    rfCount += 1
    rtCount += 1

    if rfCount % 100000 == 0:
#      print l
      print "%i,%i,%i,%i,%i,%i" %(fCount, rtCount, rfCount, len(uMacs), len(uNames),len(messages))

    reUMac = regExUMac.search(l)
    reUName = regExUName.search(l)
    reMsg = regExMsg.search(l)
    uMac=None
    uName=None
    msg = None
      
    
      

    if reMsg != None:
      msg = reMsg.group(1)

    l = l.lower()
	
    if ("authentication successful"  in l and "username=" in l and "mac=" in l)  or "deauth " in l:
      obj = processLine(l)
     

      try:
        if obj != None:
	  
          #id = cl.insert_one(obj).inserted_id
	  entries.append(obj)
	  cnt+=1
	  if cnt%10000==0:
                ids = cl.insert_many(entries).inserted_ids
	        entries = []
		print "inserted: " + str(cnt)
      except Exception as exp:
        print("-- ERROR: %s;\n  line: %s" % (str(exp), l))

        if client:
          client.close()
  gFile.close()
if len(entries)!=0:
	ids = cl.insert_many(entries).inserted_ids
	entries = [] 

errors.close()
if client:
  client.close()
print str(uNames)
print str(uMacs)
print str(messages)
