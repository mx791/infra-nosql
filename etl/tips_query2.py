import pymongo
import time
import pandas as pd
import math

myclient = pymongo.MongoClient("mongodb://192.168.3.160:27019/")
mydb = myclient["projet_nosql"]
mytable = mydb["taxis"]

destination_area =  int(input("dropoff area:"))

myquery = [
    {"$match": {"Dropoff Community Area": destination_area}},
    {"$group": {
        "_id": "$Dropoff Community Area",
        "avg_tips": {"$avg": "$Tips"},
        "avg_distance": {"$avg": "$Trip Miles"},
        "nb": {"$sum": 1}}
    }
]

chemin = '/home/ubuntu/infra-nosql/etl'
df = pd.read_csv(chemin + '/community_areas', delimiter=',', usecols=["No.", "Name"])

print("\nRecherche en cours...")

nb_execution = 10
execution_times = []

for i in range(nb_execution):
    start_time = time.time()
    results = list(mytable.aggregate(myquery))
    execution_times.append(time.time() - start_time)

    
name = df[df['No.']==int(results[0]["_id"])]["Name"].values[0]
print("Comparaison de ", nb, "trajet")
print(f"Dropoff Community Area {destination_area}:{name} avec la plus faible moyenne de tips : {results[0]['avg_tips']}")
    
print("Execution time : ", sum(execution_times)/nb_execution, "\n")
