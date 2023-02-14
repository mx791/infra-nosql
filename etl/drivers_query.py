import pymongo
import time
import pandas as pd
import matplotlib.pyplot as plt 
import numpy as np

try : 
    myclient = pymongo.MongoClient("mongodb://192.168.3.160:27019/")
except:
    myclient = pymongo.MongoClient("mongodb://192.168.3.202:27019/")
    


   
mydb = myclient["projet_nosql"]
mytable = mydb["taxis"]

# start_area = int(input("pickup area:"))
# destination_area =  int(input("dropoff area:"))

times = []

for _ in range(10):
    start = time.time()
    myquery = [
        {"$group": 
            {"_id": "$Taxi ID", 
             "avg_price": {"$avg": "$Trip Total"}, 
             "avg_time" : {"$avg": "$Trip Seconds"},
             "avg_dist" : {"$avg": "$Trip Miles"}}}
        ]



    print("\nRecherche en cours...")
    results = pd.DataFrame(list(mytable.aggregate(myquery))).dropna()
    end = time.time()
    
    times.append(np.round(end-start))
print("La query a mis en moyenne {}s à s'exécuter".format(np.mean(times)))

print("\n Top 5 conducteurs avec le prix moyen le plus élevé")
print(results.sort_values(by=['avg_price'], ascending=False).head(5))

print("\n Top 5 conducteurs avec la distance moyenne parcourie la plus élevée")
print(results.sort_values(by=['avg_dist'], ascending=False).head(5))

print("\n Top 5 conducteurs avec le temps de trajet moyen le plus élevé")
print(results.sort_values(by=['avg_time'], ascending=False).head(5))

