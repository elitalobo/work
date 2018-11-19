from bson import BSON
from bson.code import Code
from pymongo import MongoClient
# from pymongo import Connection
import re

def stateMachine(msg, oldState):
    # Based on logic developed on Vasanta Chaganti
    # State 1: Unassociated, Unauthenticated
    # State 2: Associated, Unauthenticated
    # State 3: Unassociated, Authenticated
    # State 4: Associated, Authenticated
    
    newState = 1
    
    if msg in ('132197', '500007', '501080', '501105', '501106', '501114', 
    '522010', '522030', '522043'):
        # deauth
        
        if oldState == 3:
            newState = 1
        elif oldState == 4:
            newState = 2
        else:
            newState = oldState
            
    elif msg in ('501093', '522008', '522029', '522044'):
        # auth success
        
        if oldState == 1:
            newState = 3
        elif oldState == 2:
            newState = 4
        else:
            newState = oldState
            
    elif msg in ('126066', '500021', '501102', '501104', '501113'):
        # disassoc
        
        if oldState == 2:
            newState = 1
        elif oldState == 3:
            newState = 3
        else:
            newState = oldState
            
    elif msg in ('501100'):
        # assoc success
        
        if oldState == 1:
            newState = 2
        elif oldState == 3:
            newState = 4
        else:
            newState = oldState
            
    else:
        newState = oldState
        
    return newState
    
# end of stateMachine


#########################################
# Starting MongoDB
#########################################
client = MongoClient('localhost', 27017)
# client = Connection()
odb = client['oit-db']
# odb['perUMDT'].ensureIndex({"_id":1})
# grouped = odb.getCollection('perUMDT')
flat = odb['oit_transition']

colList = odb.collection_names()

if len(colList) != 0:
        
    if "pythonUMDT" in colList:
        odb.drop_collection('pythonUMDT')

grouped = odb['perUMDT']
# grouped = odb['pythonUMDT']

mp = Code("""
    function() {
        if (this.TD != null) {
            var y = this.TD.getYear() + 1900
            var m = this.TD.getMonth() + 1
            var d = this.TD.getDate()
            
            var comp = this.UM + "_" + y 
            
            if (m < 10) {
                comp += "0"
            }
            
            comp += m
            
            if (d < 10) {
                comp += "0"
            }
            
            comp += d
            
            emit(
                comp,
                { 
                data: [ [ this.TD, this.MT, this.AN, this.UM ] ] 
                } 
            )    
        }
    }
    """)
    
rd = Code("""
    function(key,values) {
      var result = [];
      values.forEach(function(V){
        Array.prototype.push.apply(result, V.data);
      });
      return { data: result };
    }
    """)
sorter = Code(
    """function(key,values) {
      return values.data.sort(function(a,b){
        return (a[0]<b[0]) ? -1 : (a[0]>b[0]) ? 1 : 0;
      });
    }
    """)

print("Starting mapReduce...")    

# db.flat.map_reduce(mp, rd, {out: "pythonUMDT", finalize: sorter})

print("Finished mapReduce!!! (:")

if len(colList) != 0:
        
    if "presences" in colList:
        odb.drop_collection('presences')

durations = odb['presences']

if grouped != None:
    print("Success, woohoo (-:")
    # grouped.ensureIndex({"_id":1})
    bunches = grouped.find().sort([("_id", 1)])
    
    oosCounter = 0 # out of sequence counter
    errCounter = 0
    
    idRE = re.compile(r"(.+)\_(\d{8})(A|P)$")
    cID = ""
    pID = ""
    
    presences = []
    states = {}
    startTimes = {}
    endTimes = {}
    sList = []
    
    apName = None
    prevMsg = None
    sTime = None
    eTime = None
    
    for bunch in bunches:
        
        idRes = idRE.search(bunch['_id'])
        
        if idRes != None:
        
            # id = bunch['_id']
            
            cID = idRes.group(1)
            
            if pID != "" and pID != cID:
                #restarting stuff
                
                # done with trjs per user
                if len(presences) > 0:
                    presences.sort(key = lambda x:x['ST'])
                    durations.insert({"_id": pID, "presences" : presences})
            
                # done with current mac address
                # print("user: %s; open connections: %i\n..states: %s" % (id, len(states), str(sList)))
                # purging all the half baked associations  
                
                presences = []
                states = {}
                startTimes = {}
                endTimes = {}
                sList = []
                
                apName = None
                prevMsg = None
                sTime = None
                eTime = None
                
                pID = ""
                
            if pID == "" :
                # resetting pID
                print("Starting id: %s" % (cID))
                # print("  trj: %s" % (bunch['trj']))
                pID = cID
                
            else:
                # going forward with previous
                print ("Continuing id: %s" % (cID))
            
            # for i in range(len(bunch['trj'])):
            for i in range(len(bunch['value'])):
            
                try:
            
                    # ap = bunch['trj'][i]['AN']
                    # msg = bunch['trj'][i]['MT']
                    # td = bunch['trj'][i]['TD']
                    
                    ap = bunch['value'][i][2]
                    msg = bunch['value'][i][1]
                    td = bunch['value'][i][0]
                    
                    if ap not in states:
                        # the current ap doesn't have an ongoing state machine
                        
                        assert ap not in startTimes, "AP already listed in startTimes"
                        assert ap not in endTimes, "AP already listed in endTimes"               

                        if msg in ('522008','522044','522029'):
                            states[ap] = 3
                            sList.append(3)
                            
                        elif msg in ('501100'):
                            states[ap] = 2
                            sList.append(2)
                            
                        else:
                            oosCounter += 1
                            td = None
                        
                        if td != None:
                            startTimes[ap] = td
                            
                    else:
                        # the current ap already has an onging state machine
                        
                        oldState = states[ap]
                        newState = stateMachine(msg, oldState)
                        sList.append(newState)
                        
                        if oldState == newState:
                            # the state hasn't changed, means we got some weird message or sequence
                            
                            oosCounter += 1
                            
                        else:
                            
                            if newState == 1:
                                # we're disassociated and deauthenticated, current session with ap is over
                                
                                endTimes[ap] = td
                                pr = {"ET" : endTimes[ap], "ST" : startTimes[ap], "AN" : ap}
                                # print("..%s" % (str(pr)))
                                presences.append(pr)
                                
                                del endTimes[ap]
                                del startTimes[ap]
                                del states[ap]
                            
                            else:
                                states[ap] = newState
                                
                                # if oldState == 4 and newState in (2, 3):
                                if newState in (2, 3, 4) and ap not in endTimes:
                                    # expecting to continue to state 1, but prepping just in case
                                    
                                    endTimes[ap] = td
                                
                                if oldState in (2, 3) and newState == 4 and ap in endTimes:
                                    # got bounced off the ap and now re-auht/reassoc (or something of that sort?)
                                    
                                    presences.append({"ET" : endTimes[ap], "ST" : startTimes[ap], "AN" : ap})
                                    
                                    startTimes[ap] = endTimes[ap]
                                    del endTimes[ap]

                except Exception as exp:
                    errCounter += 1
                    # print("  Error: %s;\n    bunch: %s" % (str(exp), str(bunch['trj'])))                
                    print("  Error: %s;\n    bunch: %s" % (str(exp), str(bunch['value'])))                
                    
                    if client:
                        client.close()
                            
            # done with trjs per user
            # if len(presences) > 0:
                # presences.sort(key = lambda x:x['ET'])
                # durations.insert({"_id": cID, "presences" : presences})
        
            # done with current mac address
            # print("user: %s; open connections: %i\n..states: %s" % (id, len(states), str(sList)))
            # purging all the half baked associations    
                
        else:
            print("weird stuff in id: %s" % (bunch['_id']))

else:
    print("Failure, whawha )...: ")
    
if client:
    client.close()
    
if errCounter > 0:
    print("error counter: %i" % (errCounter))
    
    # eTime = ()
    # sTime = ()
    # loc = ""    
    # presence = {}
    
    # loc = msg.AN
    
    # if msg.mt == assoc:
        
        # assert sTime = "", "Association Success already happened for %s" % (loc)
        
        # sTime = msg.TD
        
    # if msg.MT == disassoc:
    
        # assert sTime != "", "No Association success for %s" % (loc)
        
        # eTime = msg.TD
        
    # if sTime != () and eTime != () and loc != "":
        # presence = {"ST" : sTime, "ET" : eTime, "AN" : loc}
    
    # return presence

