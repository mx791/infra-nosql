import pymongo

myclient = pymongo.MongoClient("mongodb://192.168.3.160:27019/")
mydb = myclient["projet_nosql"]
mytable = mydb["taxis"]

start_area = int(input("pickup area:"))
destination_area =  int(input("dropoff area:"))

myquery = [
    {"$match": {"Pickup Community Area": start_area, "Dropoff Community Area": destination_area}},
    {"$group": {
        "_id": "$Pickup Hour",
        "avg_seconds": {"$avg": "$Trip Seconds"},
        "avg_distance": {"$avg": "$Trip Miles"},
        "avg_price": {"$avg": "$Fare"},
        "nb": {"$sum": 1}}
    }
]

results = list(mytable.aggregate(myquery))

cheapest_h, cheapest_ratio, , predicted_price = 0, 100000, 0
fastest_h, fastest_ratio, predicted_time = 0, 100000, 0

for doc in results:
    if doc["_id"] != None:
        
        s_per_mile = doc["avg_seconds"] / doc["avg_distance"]
        if s_per_mile < fastest_ratio:
            fastest_h = doc["_id"]
            fastest_ratio = s_per_mile
            predicted_time = s_per_mile * doc["avg_distance"]
            
        price_per_mile = doc["avg_price"] / doc["avg_distance"]
        if s_per_mile < cheapest_ratio:
            cheapest_h = doc["_id"]
            cheapest_ratio = price_per_mile
            predicted_price = price_per_mile * doc["avg_distance"]
            
print("Le moins cher:")
print("départ à ", cheapest_h, "h, estimation: ", cheapest_ratio, "$ / mile")
print("prix estimé:", int(predicted_price), "$ \n")


print("Le plus rapide:")
print("départ à ", fastest_h, "h, estimation: ", fastest_ratio, "seconds / mile")
print("temps estimé:", int(predicted_time / 60), "m, "\n")
