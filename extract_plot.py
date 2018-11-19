import os
import sys
dir = "logs/"
import subprocess
import re
import gzip 

import sqlite3
conn = sqlite3.connect('iodatabase')

def remove_white_spaces(m):
        temp = []
        for x in m:
                if x.strip()!="":
                        temp.append(x.strip())
        return temp




regExUMac = re.compile(r".+>.+#(.+)#")
regExUName = re.compile(r".+>.+%(.+)%")
regExMsg = re.compile(r".+<(\d+)>")

fCount = 0
rtCount = 0
rfCount = 0

mac_messages=[]
mac_aps={}
from datetime import datetime
#subprocess.call(cmd,shell=True)
print "done!!!"
r= open("logs.txt",'r')
for x in r:
	reUMac = regExUMac.search(x)
	reUMac = reUMac.group(1)
	if mac_aps.get(reUMac)==None:
		mac_aps[reUMac]=set([])
	mac_messages.append(reUMac)
	k = x.split(" ")
	AP = k[-8][3:]
	mac_aps[reUMac].add(AP)

mac_messages=[]
maxm=0
mac_id=None
for x,y in mac_aps.items():
	if len(mac_aps[x])> maxm:
		maxm=len(mac_aps[x])
		mac_id= x
	
mac_messages.append(mac_id)
print mac_id
	
final_messages={}
final_messages[mac_id]=[]	
	#mac_messages.append("7Twmzq6q.t2BL0JohdWx9w")	
	#final_messages["7Twmzq6q.t2BL0JohdWx9w"]=[]
start = None
end  = None
for x in mac_messages:
	#cmd = "zgrep " + x + " " + dir  + "*" + "  > temp_"+user
	#print cmd
	#subprocess.call(cmd,shell=True)
	r= open("logs.txt",'r')
	for z in r:
		try:
			m = z.split(" ")
			m = remove_white_spaces(m)
			req = m[0].split(":")
			lent = len(req)-1
			dt = "2017 "+ req[lent] + " " + m[1] + " " + m[2]
			dt = datetime.strptime(str(dt),"%Y %b %d %H:%M:%S")		
			if start!= None and "deauth" in z.lower():
				import ipdb; ipdb.set_trace()
				print "Found end"
				print dt
				final_messages[x].append((start,dt))
				start=None
			if " user authentication successful" in z.lower() and start==None:
				start = dt
				print "Found start" 
				print start
		except:
			import ipdb; ipdb.set_trace()
			print "did nothing"
			print z
	break
	
k = open("user_sessions_temp",'w+')
k.write("start,end\n")
for x,y in final_messages.items():
	for z in y:
		k.write(str(z[0].date()) + " " + str(z[0].time()) + "," + str(z[1].date()) + " " + str(z[1].time())+"\n")
k.close() 
	
	
			

	
#  print "done with reading gzip"
'''  rfCount = 0

  for l in gFile:
    print l
    rfCount += 1
    rtCount += 1
    if rfCount==20:
        break
    if rfCount % 1000000 == 0:
#      print l
      print "%i,%i,%i,%i,%i,%i" %(fCount, rtCount, rfCount, len(uMacs), len(uNames),len(messages))

    reUMac = regExUMac.search(l)
    reUName = regExUName.search(l)
    reMsg = regExMsg.search(l)

    if reUMac != None:
      uMac = reUMac.group(1)

      if uMac in uMacs:
        uMacs[uMac] += 1
      else:
        uMacs[uMac] = 1

    if reUName != None:
      uName = reUName.group(1)

      if uName in uNames:
        uNames[uName] += 1
      else:
        uNames[uName] = 1

    if reMsg != None:
      msg = reMsg.group(1)

      if msg in messages:
        messages[msg] += 1
      else:
        messages[msg] = 1

  gFile.close()

'''
