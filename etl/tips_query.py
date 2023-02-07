import pymongo
import time
import pandas as pd

myclient = pymongo.MongoClient("mongodb://192.168.3.160:27019/")
mydb = myclient["projet_nosql"]
mytable = mydb["taxis"]

myquery = [
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

min_tips_h, min_tips = 0, 100000
max_tips_h, max_tips = 0, 0

total_tips = 0
nb = 0
nb_community_area = 0

for doc in results:
    if doc["_id"] != None:
        if min_tips >= doc["avg_tips"]:
            min_tips_h = doc["_id"]
            min_tips = doc["avg_tips"]
        elif max_tips <= doc["avg_tips"]:
            max_tips_h = doc["_id"]
            max_tips = doc["avg_tips"]
        nb += doc["nb"]
        total_tips += doc["avg_tips"]
        nb_community_area += 1
    
min_name = df[df['No.']==int(min_tips_h)]["Name"].values[0]
max_name = df[df['No.']==int(max_tips_h)]["Name"].values[0]
print("Comparaison de ", nb, "trajet")
print(f"Dropoff Community Area {min_tips_h}:{min_name} avec la plus faible moyenne de tips : {min_tips}")
print(f"Moyenne tips sur toutes les Community Areas {total_tips/nb_community_area}")
print(f"Dropoff Community Area {max_tips_h}:{max_name} avec la plus forte moyenne de tips : {max_tips}\n")
    
print("Execution time : ", sum(execution_times)/nb_execution, "\n")
