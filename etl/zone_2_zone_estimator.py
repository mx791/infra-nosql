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

cheapest_h, cheapest_ratio = 0, 100000
fastest_h, fastest_ratio = 0, 100000

for doc in results:
    if results["_id"] != None:
        
        s_per_mile = results["avg_seconds"] / results["avg_distance"]
        if s_per_mile < fastest_ratio:
            fastest_h = results["_id"]
            fastest_ratio = s_per_mile
            
        price_per_mile = results["avg_price"] / results["avg_distance"]
        if s_per_mile < cheapest_ratio:
            cheapest_h = results["_id"]
            cheapest_ratio = s_per_mile
            
print("Le moins cher:")
print("départ à ", cheapest_h, "h, estimation: ", cheapest_ratio, "$ / mile")


print("Le plus rapide:")
print("départ à ", fastest_h, "h, estimation: ", fastest_ratio, "seconds / mile")
