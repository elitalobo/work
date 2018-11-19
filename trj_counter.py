import gzip
import glob
import re

uMacs = {}
uNames = {}
messages = {}

fList = []
fList = glob.glob("*.gz")
fList.sort()
#fList.append("NET-wifi-all.log.20131128.encrypted.gz")

print str(len(fList))
#print str(fList)

regExUMac = re.compile(r".+>.+#(.+)#")
regExUName = re.compile(r".+>.+%(.+)%")
regExMsg = re.compile(r".+<(\d+)>")

fCount = 0
rtCount = 0
rfCount = 0

for f in fList:
  fCount += 1
  print "file #: %i; %s" % (fCount, f)

  gFile = gzip.open(f, 'rb')  
  
#  print "done with reading gzip"
  rfCount = 0
  
  for l in gFile:
    rfCount += 1
    rtCount += 1

    if rfCount % 10000 == 0:
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

  if fCount == 5:
    break

print str(uNames)
print str(uMacs)
print str(messages)
