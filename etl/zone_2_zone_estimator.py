import pymongo

myclient = pymongo.MongoClient("mongodb://192.168.3.160:27019/")
mydb = myclient["projet_nosql"]
mytable = mydb["taxis"]

start_area = 5
destination_area = 6

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

print(mytable.aggregate(myquery))
