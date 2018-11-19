# from pymongo import Connection
from pymongo import MongoClient
from datetime import datetime as dt
from math import ceil
import matplotlib.pyplot as plt

client = MongoClient('localhost', 27017)
odb = client['oit-db']
cl_list = odb.collection_names()

if len(cl_list) != 0:
    col = odb['presences']
    
    if col != None:
        pres_dur = {}
        gap_dur = {}
        users = col.find()
        
        pDur = 0
        gDur = 0
        
        pst = 0
        pet = 0
        
        for user in users:
            pst = 0
            pet = 0
            
            cst = 0
            cet = 0
            
            for pr in user['presences']:
                cst = pr['ST']
                cet = pr['ET']
                
                pDur = cet - cst
                
                # print(cst, cet, pDur)
                
                if pst != 0 and pet != 0:                    
                    gDur = cst - pet
                    
                pst = cst
                pet = cet
                
                if pDur != 0:
                    mins = ceil(pDur.seconds / 60)
                    
                    if pDur.days < 0:
                        mins *= -1
                else:
                    mins = 0
                
                if mins < 0:
                    tag = -10
                elif mins >= 0 and mins <= 60:
                    tag = mins
                elif mins > 60 and mins <= 120:
                    tag = 120
                elif mins > 120 and mins <= 180:
                    tag = 180
                elif mins > 180 and mins <= 240:
                    tag = 240
                elif mins > 240 and mins <= 300:
                    tag = 300
                elif mins > 300 and mins <= 360:
                    tag = 360
                elif mins > 360 and mins <= 420:
                    tag = 420
                elif mins > 420 and mins <= 480:
                    tag = 480
                else:
                    tag = 1000
                
                if tag in pres_dur:
                    pres_dur[tag] += 1
                else:
                    pres_dur[tag] = 1
                
                if gDur != 0:
                    mins = ceil(gDur.seconds / 60)
                    
                    if gDur.days < 0:
                        mins *= -1
                else:
                    mins = 0
                
                if mins < 0:
                    tag = -10
                elif mins >= 0 and mins <= 60:
                    tag = mins
                elif mins > 60 and mins <= 120:
                    tag = 120
                elif mins > 120 and mins <= 180:
                    tag = 180
                elif mins > 180 and mins <= 240:
                    tag = 240
                elif mins > 240 and mins <= 300:
                    tag = 300
                elif mins > 300 and mins <= 360:
                    tag = 360
                elif mins > 360 and mins <= 420:
                    tag = 420
                elif mins > 420 and mins <= 480:
                    tag = 480
                else:
                    tag = 1000
                
                if tag in gap_dur:
                    gap_dur[tag] += 1
                else:
                    gap_dur[tag] = 1
    else:
        print("No can has collections ): ")
else:
    print("Cannot connect )...:")
    
if client:
    client.close()

print("\nPresence Durations:\n%s" % (str(pres_dur)))
print("\nGap Durations:\n%s" % (str(gap_dur)))
    
with plt.xkcd():
    fig = plt.figure(1)
    fig.set_facecolor('white') 
    
    plt.subplot(211)    
    plt.xlabel("Duration Values [min]")
    plt.ylabel("Counts")
    plt.title("Counts of Presence Durations" )
    plt.bar(list(pres_dur.keys()), list(pres_dur.values()), color='red')    

    plt.subplot(212)
    plt.xlabel("Duration Values [min]")
    plt.ylabel("Counts")    
    plt.title("Counts of Gap Durations " )
    plt.bar(list(gap_dur.keys()), list(gap_dur.values()), color='green')    
    
    plt.show()