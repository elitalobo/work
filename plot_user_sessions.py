import os
from datetime import datetime
import sys
import numpy as np
import matplotlib as mpl
if os.environ.get('DISPLAY','') == '':
    print('no display found. Using non-interactive Agg backend')
    mpl.use('Agg')
import matplotlib.pyplot as plt
filename = sys.argv[1]
r= open(filename,'r')
xt=[]
yt=[]
c=""
xt_=[]
start=1
flag = False
for x in r:
	dates = (x.strip()).strip("\n").split(",")
	if flag== False:
		flag = True
		continue
	for y in dates:	
		cur = y.split(" ")[0]
		dt = datetime.now()
		#dt = datetime.strptime(cur + " " + y.split(" ")[1], "%Y-%m-%d %H:%M:%S")
		#print "current: " + str(cur)
		#print "c: " + str(c)
		if c=="":	
			c = cur
		elif c!= cur:
			print "current not equal"
			print c
			print cur
			c= cur
			start+=1	
		y = (y.split(" "))[1].split(":")
		lent = len(xt)
		prev = None
		if len(xt)!=0:
			prev = xt[len(xt)-1]
		dt  = dt.replace(hour=int(y[0]), minute=int(y[1]))
		if prev!=None and prev > dt and len(xt)%2==1:
			temp = dt.replace(hour=23, minute=59)
			xt.append(temp)
			yt.append(start-1)
			temp = dt.replace(hour=0, minute=0)
			xt.append(temp)
			yt.append(start)
                dt  = dt.replace(hour=int(y[0]), minute=int(y[1]))

		xt.append(dt)
		yt.append(start)
import ipdb; ipdb.set_trace()
import numpy as np
print  np.max(yt)
xt=xt[:-1]
yt=yt[:-1]	
xt= np.array(xt)
yt=np.array(yt)
fig = plt.gcf()
import matplotlib.dates as mdates

plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
#plt.gca().xaxis.set_major_locator(mdates.DayLocator())
#plt.plot(x,y)
#fig.autofmt_xdate()
plt.xlabel("hours")
plt.ylabel("days")
plt.title("User wifi activity")
fig.set_size_inches(24.5, 10.5)
for i in range(0, len(xt), 2):
    plt.plot(xt[i:i+2], yt[i:i+2], '-')
    plt.xticks(rotation=45)

plt.savefig(filename.split("_")[2]+ "_wifi_usage.png")
