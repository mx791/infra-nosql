import pymongo
import time

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

print("\nRecherche en cours...")
start_time = time.time()

results = list(mytable.aggregate(myquery))

end_time = time.time()

min_tips_h, min_tips = 0, 100000
max_tips_h, max_tips = 0, 0

nb = 0

for doc in results:
    if doc["_id"] != None:
        if min_tips >= doc["avg_tips"]:
            min_tips_h = doc["_id"]
            min_tips = doc["avg_tips"]
        elif max_tips <= doc["avg_tips"]:
            max_tips_h = doc["_id"]
            max_tips = doc["avg_tips"]
        nb += doc["nb"]
    
            
print("Comparaison de ", nb, "trajet\n")
print(f"Dropoff Community Area {min_tips_h} avec la plus faible moyenne de tips : {min_tips}\n")
print(f"Dropoff Community Area {max_tips_h} avec la plus forte moyenne  de tips : {max_tips}\n")
    
print("Execution time : ", time.time() - start_time, "\n")