import pymongo

myclient = pymongo.MongoClient("mongodb://192.168.3.160:27019/")
mydb = myclient["projet_nosql"]
mytable = mydb["taxis"]


myquery = [
    {"$match": {"Pickup Community Area": 5, "Dropoff Community Area": 6}},
    {"$group": {
        "_id": "$Pickup Hour",
        "avg_seconds": {"$avg": "$Trip Seconds"},
        "avg_distance": {"$avg": "$Trip Miles"},
        "avg_price": {"$avg": "$Fare"},
        "nb": {"$sum": 1}}
    }
]

print(mytable.aggregate(myquery))
