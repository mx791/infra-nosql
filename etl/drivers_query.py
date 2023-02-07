import pymongo
import pandas as pd
import matplotlib.pyplot as plt 


myclient = pymongo.MongoClient("mongodb://192.168.3.160:27019/")
mydb = myclient["projet_nosql"]
mytable = mydb["taxis"]

# start_area = int(input("pickup area:"))
# destination_area =  int(input("dropoff area:"))

myquery = [
    {"$group": 
        {"_id": "$Taxi ID", 
         "avg_price": {"$avg": "$Trip Total"}, 
         "avg_tips" : {"$avg": "$Tips"}, 
         "avg_time" : {"$avg": "$Trip Seconds"}}}
    ]

print("\nRecherche en cours...")
results = pd.DataFrame(list(mytable.aggregate(myquery)))
print(results.sort_values(by=['avg_price'], ascending=False).head(5))
print(results.sort_values(by=['avg_tips'], ascending=False).head(5))

# cheapest_h, cheapest_ratio, cheapest_time = 0, 100000, 0
# fastest_h, fastest_ratio, fast_price = 0, 100000, 0

# mean_distance = 0

# for doc in results:
    
#     if doc["_id"] != None:
        
#         mean_distance += doc["avg_distance"] / len(results)
        
#         s_per_mile = doc["avg_seconds"] / doc["avg_distance"]
#         if s_per_mile < fastest_ratio:
#             fastest_h = doc["_id"]
#             fastest_ratio = s_per_mile
#             fast_price = doc["avg_price"] / doc["avg_distance"]
            
#         price_per_mile = doc["avg_price"] / doc["avg_distance"]
#         if price_per_mile < cheapest_ratio:
#             cheapest_h = doc["_id"]
#             cheapest_ratio = price_per_mile
#             cheapest_time = doc["avg_seconds"] / doc["avg_distance"]
            
#         nb += doc["nb"]
            
# print("Comparaison de ", nb, "trajet\n")
# print("Estimation de la distance: ", mean_distance, "miles \n")
            
# print("Le moins cher:")
# print("départ à ", cheapest_h, "h, estimation: ", cheapest_ratio, "$ / mile")
# print("prix estimé:", mean_distance * cheapest_ratio, "$")
# print("temps estimé:", mean_distance * cheapest_time / 60, "minutes \n")

# print("Le plus rapide:")
# print("départ à ", fastest_h, "h, estimation: ", fastest_ratio, "seconds / mile")
# print("prix estimé:", mean_distance * fast_price, "$")
# print("temps estimé:", fastest_ratio * mean_distance / 60, "minutes \n")
