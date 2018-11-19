from bson import BSON
# from pymongo import MongoClient
from pymongo import Connection
import sys
import re
import glob
import gzip
from datetime import datetime as dt

""" 
------------------------------------------------------------
Aruba syslog structure:

Lines that will be ignored contain "KERNEL" - some system message that 
doesn't contain information about the users;

Other than that, typical line structure:

Mon mday hh:mm:ss controller_name process_name[process_id]: <message_subtype> <message_type> _
  <controller_name controller_ip> message_string
  
(1) Mon - the letter month, first capital
(2) mday - date in the month (fixed to double digit?!)
(3) hh:mm:ss - timestamp, 24 hour format
(4) controller_name - name of one of 10-12 controller, each controlling up to 512 access points
(5) process_name - name of process issuing the message
(6) process_id - numerical CPU process id (issued by central server ?!)
(7) <message_subtype> - unique numerical identification of message
(8) <message_type> - 4 letter encoding of message type (WARN, NOTI, ERRS etc.)
(9) controller_name - same as earlier
(10) controller_ip - the IP address of the controller
(11) message_string - a detailed message containing additional information, contains spaces
-----------------------------------------------------------
"""

def setFilename(inFile, tm):
    """ Creates a name for a file that contains log info of up to half an hour """
    
    fNameRe = re.compile(r"(\S+)\.(\w+)$")
    fParts = fNameRe.search(inFile)
    
    fName = fParts.group(1) + "_" + tm.group(1) + tm.group(2) + "." + fParts.group(2)    
    
    return fName
# End of setFilename

def processBSON(line):
    """ Process an Aruba SysLog line into a BSON object for MongoDB"""
    
    mObj = {}
    tObj = {}
    trans = False
    
    mtEX = re.compile(r"<(\d{6})>")
    msHit = mtEX.search(line)
    
    if msHit != None:
    
        if msHit.group(1) in ('501068',
        '404098', '404097', '404094', '404093', 
        '404089', '404085',
        '404075', '404074', '404071', '404070', 
        '404069', '404068', '404065', 
        '404050', 
        '334302', '326085', '311002', 
        '303086', '303022',
        '126005', '126035', '126047', '126066', '126069', '126071', '126085', '126087', '126109',
        '121004',
        '106001'): 
            # print msHit.group(1)
            pass
        
        else:
        
            pipeEX = re.compile(r"\>\s+\|")
            pipeRes = pipeEX.search(line)
            
            if pipeRes == None:    
                lineEx = re.compile(r"([A-Z][a-z]{2})\s+(\d+)\s+(\d{2}\:\d{2}\:\d{2})\s+([\w\-]+)\s(\w+)\[(\d+)\]\:\s+\<(\d+)\>\s+\<(\w+)\>\s+\<([\w\-]+)\s+\*(\S+)\*\>\s+(.*)")
            else:
                lineEx = re.compile(r"([A-Z][a-z]{2})\s+(\d+)\s+(\d{2}\:\d{2}\:\d{2})\s+\*(.+)\*\s(\w+)\[(\d+)\]\:\s+\<(\d+)\>\s+\<(\w+)\>\s+\|AP\s+(.+)\@\*(\S+)\*\s+\S+\|\s+(.*)")
            lineRes = lineEx.search(line)
            
            if lineRes == None:
                print(line)
            
            else:
            
                mon = lineRes.group(1)
                mday = lineRes.group(2)    
                timeStamp = lineRes.group(3)    

                procName = lineRes.group(5)    
                procID = lineRes.group(6)
                    
                msgType = lineRes.group(7)
                msgCat = lineRes.group(8)

                cName = lineRes.group(4)
                cntrlName = lineRes.group(9)
                cntrlIP = lineRes.group(10)

                assert(cName == cntrlName) or (cName == cntrlIP), "Controller names are not the same!! header: %s ; later: %s" % (cName, cntrlName)

                msgTxt = lineRes.group(11)

                # tmRE = re.compile(r"\d\d:\d\d:\d\d\.(\d+)")
                # micros = tmRE.search(msgTxt)

                year = "2013"

                if (mon in ("Jan", "Feb")):
                    year = "2014"
                    
                tdString = year + " " + mon + " " + mday + " " + timeStamp
                
                # if micros != None:
                    # tdString += " " + micros.group(1)
                    # td = dt.strptime(tdString, "%Y %b %d %H:%M:%S %f")
                # else:
                td = dt.strptime(tdString, "%Y %b %d %H:%M:%S")
                
                # TD = TimeDate, MC = MessageCategory, MT = MessageType
                # CN = ControllerName, CIP = ControllerIP
                # PID = ProcessID, PN = ProcessName, Msg = MessageDetails
                
                mObj = {"TD" : td, 
                "MC" : msgCat, "MT" : msgType,
                "CN" : cntrlName, "CIP" : cntrlIP,
                "PID" : procID, "PN" : procName,
                "Msg" : msgTxt}
                
                if msgType in ('132197', 
                '501044', '501080', '501093', '501095',
                '501100', '501102', '501105', '501106', '501109', '501114',
                '522008', '522010', '522029', '522043', '522044'):
                    # 522030, 500007, 501104, 501113, 500021
                    # 126066 - is it an actual disassoc?
                    trans = True
                    
                    uMAC = ""
                    uIP = ""
                    apName = ""
                    
                    if msgType == "132197":
                        #  132197: Max number retries, deauth
                        pRE = re.compile(r"\#(.+)\#\s+\#(.+)\#")
                        pRes = pRE.search(msgTxt)
                        
                        if pRes != None:
                            uMAC = pRes.group(1)
                            apMAC = pRes.group(2)
                            
                            tObj = tObj = {"TD" : td, "MT" : msgType, "UM" : uMAC, "UIP" : uIP, 
                            "AMC": apMAC, "AN" : apName}
                    
                    if msgType == "501044":
                        # 501044: No authentication found trying to de-authenticate to BSSID
                        pRE = re.compile(r"\#(.+)\#.+\#(.+)\#.+AP\s(.+)")
                        pRes = pRE.search(msgTxt)
                        
                        if pRes != None:
                            apMAC = pRes.group(1)
                            uMAC = pRes.group(2)
                            apName = pRes.group(3)
                            
                            tObj = {"TD" : td, "MT" : msgType, "UM" : uMAC, "UIP" : uIP,
                            "AMC": apMAC, "AN" : apName}
                    
                    if msgType in ('501080', '501102', '501106', '501114'):
                        # 501080: Deauth to sta; Ageout; STA left and is deauth / APAE Disconnect
                        # 501102: Disassoc from sta
                        # 501106: Deauth to sta; Ageout; wifi_deauth_sta
                        # 501114: Death from sta
                        pRE = re.compile(r".*\#(\S+)\#:.+AP\s+\*(\S+)\*\-\#(\S+)\#\-(\S+)\s+(.+)")
                        pRes = pRE.search(msgTxt)
                        
                        if pRes != None:                        
                            uMAC = pRes.group(1)
                            apIP = pRes.group(2)
                            apMAC = pRes.group(3)
                            apName = pRes.group(4)
                            reason = pRes.group(5)
                        
                            tObj = {"TD" : td, "MT" : msgType, "UM" : uMAC, "UIP" : uIP,
                            "AIP" : apIP, "AMC" : apMAC, "AN" : apName, "RN" : reason}
                    
                    if msgType == "501095":
                        # 501095: Assoc request
                        pRE = re.compile(r".*@\s+\d{2}:\d{2}:\d{2}\.\d+:\s+\#(\S+)\#\s+\((.+)\):\s+AP\s+\*(\S+)\*\-\#(\S+)\#\-(\S+)")
                        pRes = pRE.search(msgTxt)
                        
                        if pRes != None:                        
                            uMAC = pRes.group(1)
                            sn = pRes.group(2)
                            apIP = pRes.group(3)
                            apMAC = pRes.group(4)
                            apName = pRes.group(5)
                        
                            tObj = {"TD" : td, "MT" : msgType, "UM" : uMAC, "UIP" : uIP,
                            "AIP" : apIP, "AMC" : apMAC, "AN" : apName, "SN": sn}
                    
                    if msgType in ('501093', '501100'):
                        # 501093: Auth succes
                        # 501100: Assoc Success
                        pRE = re.compile(r".+\@\s+(\d{2}\:\d{2}\:\d{2}\.\d{1,6})\:\s+\#(.+)\#.+\*(.+)\*\-\#(.+)\#\-(.+)")
                        pRes = pRE.search(msgTxt)
                        
                        if pRes != None:
                            # not matching TD, already done earlier
                            uMAC = pRes.group(2)
                            apIP = pRes.group(3)
                            apMAC = pRes.group(4)
                            apName = pRes.group(5)
                        
                            tObj = {"TD" : td, "MT" : msgType, "UM" : uMAC, "UIP" : uIP,
                            "AIP" : apIP, "AMC" : apMAC, "AN" : apName}
                    
                    if msgType == "501105":
                        # 501105: Deauth from sta: STA has left / Response to challenge failed
                        pRE = re.compile(r".*\#(\S+)\#:.+AP\s+\*(\S+)\*\-\#(\S+)\#\-(\S+)\s+(.+)")
                        pRes = pRE.search(msgTxt)
                        
                        if pRes != None:                        
                            uMAC = pRes.group(1)
                            apIP = pRes.group(2)
                            apMAC = pRes.group(3)
                            apName = pRes.group(4)
                            reason = pRes.group(5)
                        
                            tObj = {"TD" : td, "MT" : msgType, "UM" : uMAC, "UIP" : uIP,
                            "AIP" : apIP, "AMC" : apMAC, "AN" : apName, "RN" : reason}
                    
                    if msgType == '501109':                        
                        # 501109: Auth request                   
                        pRE = re.compile(r".*\#(\S+)\#:.+AP\s+\*(\S+)\*\-\#(\S+)\#\-(\S+)\s+(.+)")
                        pRes = pRE.search(msgTxt)
                        
                        if pRes != None:                        
                            uMAC = pRes.group(1)
                            apIP = pRes.group(2)
                            apMAC = pRes.group(3)
                            apName = pRes.group(4)
                            algo = pRes.group(5)
                        
                            tObj = {"TD" : td, "MT" : msgType, "UM" : uMAC, "UIP" : uIP,
                            "AIP" : apIP, "AMC" : apMAC, "AN" : apName, "AA" : algo}
                    
                    if msgType == "522008":
                        # 522008: User Auth Succ
                        pRE = re.compile(r".*username\=\%(\S+)\%\s+MAC\=\#(\S+)\#\s+IP\=\*(\S+)\*\s+role\=(.+)\s+VLAN\=(.+)\s+AP\=(.+)\s+SSID\=(.+)\s+profile\=(.+)\s+auth\s+method\=(.+)\s+auth\s+server\=(.+)")
                        pRes = pRE.search(msgTxt)
                        
                        if pRes != None:                        
                            uName = pRes.group(1)
                            uMAC = pRes.group(2)
                            uIP = pRes.group(3)
                            role = pRes.group(4)
                            vlan = pRes.group(5)                            
                            apName = pRes.group(6)
                            ssid = pRes.group(7)
                            profile = pRes.group(8)
                            authMeth = pRes.group(9)
                            authSrv = pRes.group(10)
                        
                            tObj = {"TD" : td, "MT" : msgType, 
                            "UN" : uName, "UM" : uMAC, "UIP" : uIP,
                            "AN" : apName,
                            "RL" : role, "VL" : vlan, "SS" : ssid,
                            "PR" : profile, "AM" : authMeth, "AS" : authSrv}
                    
                    if msgType == "522010":
                        # 522010: User de-auth; user request / session timeout
                        pRE = re.compile(r".*\#(\S+)\#:.+AP\s+\*(\S+)\*\-\#(\S+)\#\-(\S+)\s+(.+)")
                        pRes = pRE.search(msgTxt)
                        
                        if pRes != None:                        
                            uMAC = pRes.group(1)
                            uIP = pRes.group(2)
                            uName = pRes.group(3)
                            reason = pRes.group(4)                            
                        
                            tObj = {"TD" : td, "MT" : msgType, "UM" : uMAC, 
                            "UIP" : uIP, "UN" : uName, "RN" : reason, "AN" : apName}
                            
                    if msgType == "522043":
                        # 522043: Configured session limit reached
                        pRE = re.compile(r"IP=\*(.+)\*")
                        pRes = pRE.search(msgTxt)
                        
                        if pRes != None:
                            uIP = pRes.group(1)
                            
                            tObj = {"TD" : td, "MT" : msgType, "UM" : uMAC, "UIP" : uIP, "AN": apName}
                            
                    if msgType in ('522029', '522044'):
                        # 522044: Auth start
                        # 522029: Station auth
                        pRE = re.compile(r"MAC=\#(.+)\#\s+.+\:\s+method=(.+),\s+role=(.+),\s+VLAN=(.+), Derivation=(.+),\s+Value\s+Pair=(.+)")
                        pRes = pRE.search(msgTxt)
                        
                        if pRes != None:
                            uMAC = pRes.group(1)
                            authMeth = pRes.group(2)
                            role = pRes.group(3)
                            vlan = pRes.group(4)
                            deriv = pRes.group(5)
                            valPair = pRes.group(6)
                            
                            tObj = {"TD" : td, "MT" : msgType, "UM" : uMAC, "UIP" : uIP, "AN" : apName,
                            "AM" : authMeth, "RL" : role, "VL" : vlan, 
                            "DR" : deriv, "VP" : valPair}
                
    return mObj, trans, tObj
    
# End of processBSON

# if sys.argv != None and len(sys.argv) > 1:    
    # oitName = sys.argv[1]
# else:
    # oitName = "NET-wifi-all.log.20131129.encrypted"
# oitName = "anon-wifi-20140120-2000-2059.txt"
fList = glob.glob("*.gz")
fList.sort() 

# oitFile = open(oitName, 'r')

# fLine = oitFile.readline()

# timeReg = re.compile(r"\s(\d+):(\d+):\d+\s")
# timeRes = timeReg.search(fLine)

# fileTime = 0

# if int(timeRes.group(2)) >= 30: # Only checking every half hour
    # fileTime = 30
    
# smallName = setFilename(oitName, timeRes)
# smallFile = open(smallName, 'w')
# smallFile.write(fLine)

#########################################
# Starting MongoDB
#########################################
# client = MongoClient('localhost', 27017)
client = Connection()
odb = client['oit-db']
cl_list = odb.collection_names()

if len(cl_list) != 0:
    # if "oit_trace" in cl_list:
        # odb.drop_collection('oit_trace')
        
    if "oit_transition" in cl_list:
        odb.drop_collection('oit_transition')

cl_general = odb['oit_trace']
cl_transition = odb['oit_transition']

kReg = re.compile(r"KERNEL")

fCount = 0
rtCount = 0
rfCount = 0

# for fLine in oitFile:
    
    # if kReg.search(fLine) == None:
        # mobj, transition, tobj = processBSON(fLine)
        
        # if len(mobj) > 0:
            # cl_general.insert(mobj)
            
            # if len(tobj) > 0:
                # cl_transition.insert(tobj)

for f in fList:
    fCount += 1
    print("File #: %i; %s" % (fCount, f))

    rfCount = 0
    gFile = gzip.open(f, 'rb')

    for fLine in gFile:
        rfCount += 1
        rtCount += 1

        if rfCount % 10000 == 0:
            # print l
            print("%i,%i,%i" %(fCount, rtCount, rfCount))

        if kReg.search(fLine) == None:
            mobj, transition, tobj = processBSON(fLine)
            
            try:
                if len(mobj) > 0:
                    cl_general.insert(mobj)

                    if transition:
                        cl_transition.insert(tobj)
            
            except Exception as exp:
                print("-- ERROR: %s;\n  line: %s" % (str(exp), fLine))
                
                if client:
                    client.close()
                    
    gFile.close()
    
    if fCount == 5:
        break
    
print("General DB count: %i" % (cl_general.find().count()))
print("Transitoin DB count: %i" % (cl_transition.find().count()))
# if oitFile:
    # oitFile.close()
    
if client:
    client.disconnect()
    
    