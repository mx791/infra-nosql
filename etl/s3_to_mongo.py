import pymongo
import httpx
import pandas as pd
from io import StringIO
from datetime import datetime

    
myclient = pymongo.MongoClient("mongodb://192.168.3.160:27019/")
mydb = myclient["projet_nosql"]
mytable = mydb["taxis"]

#mytable.delete_many({})
COUNT = 0

# Chargement des dataframes dans Mongo
def load(dataframe):
    
    print("Analyse de ", len(dataframe), "lignes")
    
    errors = 0
    loaded = 0
    columns = dataframe.columns
    dictionnary_list = []
    
    for id in range(len(dataframe)):

        dt = dataframe["Trip Start Timestamp"][id]
        
        try:
            dict = {}
            try:
                for col in columns:
                    dict[col] = dataframe[col][id]

                dict["Pickup Hour"] = datetime.strptime(dataframe["Trip Start Timestamp"][id], "%m/%d/%Y %I:%M:%S %p").hour
                dict["Pickup Year"] = datetime.strptime(dataframe["Trip Start Timestamp"][id], "%m/%d/%Y %I:%M:%S %p").year
                dict["Pickup Month"] = datetime.strptime(dataframe["Trip Start Timestamp"][id], "%m/%d/%Y %I:%M:%S %p").month

                dictionnary_list.append(dict)
                loaded += 1
            except:
                pass

            if len(dictionnary_list) % 5000 == 0:
                mytable.insert_many(dictionnary_list)
                print("insertion de ", len(dictionnary_list), "lignes", loaded)
                dictionnary_list = []
        except:
            errors += 1
    
    return {"good": len(dictionnary_list), "errors": errors}

# Téléchargement des fichiers de S3
def extract_transform(file_url):

    print("Telechargement du fichier")

    # limite la taille de telechargement pour le tests
    truncate_after = int(1 * 1024  * 1024 * 1024)

    last_index = 0
    with httpx.stream("GET", file_url) as response:
        body = ""
        for chunk in response.iter_bytes():
            body += str(chunk)
            if int(response.num_bytes_downloaded / truncate_after * 100) != last_index:
                last_index = int(response.num_bytes_downloaded / truncate_after * 100)
                print("Telechargement:", last_index , "%, ", int(response.num_bytes_downloaded/(1024 * 1024)), "Mb /", int(truncate_after/(1024*1024)))
            if response.num_bytes_downloaded >= truncate_after:
                break
    
    # bug d'encodage -> on remplace les sauts de ligne par un autre char
    body = body.replace("\\n", ";")
    body = body.replace("'b'", "")
    body = body.replace("b'", "")
    body = body.replace("'b", "")
    
    lines = body.split(";")

    CHUNKS = 150000
    for ids in range(1, len(lines), CHUNKS):
        try:
            df = pd.read_csv(StringIO(lines[0] + ";" + ";".join(lines[ids:ids+CHUNKS])), lineterminator=";")
            load(df)
        except:
            print("erreur avec le chunk", ids)

    return df

# liste des liens des partage des fichiers sur le S3
s3_file_liste = [
    "https://nosql-taxis.s3.eu-west-3.amazonaws.com/taxi_trips_06.csv?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEH0aCWV1LXdlc3QtMyJIMEYCIQC4qu%2FD3OVmQZOAHmXmdRpevY6qGyA79AHBrcCwcZoqqQIhAKGePw2Mbmb4s712APtwGFijwyx%2BSc3RzMSailIIREPbKuQCCBYQAhoMNDUzMTgwODI2MzM3IgySUovDKXvI9Ug8PJUqwQIpboLWLxjFsDdJ2cAftCLdynHx2QOQmlIY363papC4pP7uDNMdMuEv%2BwMBDsfRfIqU%2Bs%2BCzh7aL2%2FMpfVoh4kZfPGEKYhLlQjEc5cZEdnypVaR5ldKIgCCRq1cURvOA0fQOhces%2BfAX3rnfPsGMVZ4gBHADhAmtlTp6NGL4dKNjRGpvJi7XW1mgKgNtep%2FS6VvmIpzSfQ%2BHZbLXw42tGMKm6G1xuAPAeWpTW%2FVKhhnd2jM6YiK1Qv3Hdj%2FFY6MGUWX4UYQ22TAfKrwNuWzR1DARbnF7N1J4%2BDEZnQl4W02G6a48btxPiLco0%2FDPGFuliYymIH9DRBhER3AkboSHd%2FVMnZLlxvOJF6NP%2Bvh8goB82P%2B1gNLz5XA7SQ3M6v8jMJt3AjLa7He%2FKOKVNDI%2BLsF0wCwdkIzg3ajZCYMAZuznQAwyYSunwY6sgJtgEGfAS2mvCSCvCJMZdr7FP1%2B%2BQovTPpPTWsziGK77gUGfFvwB4bI6pf%2B2BUySWSM1Ze961xwzG6J0P%2FVL0QPEujjLToPdMe2opK6dTHn%2BB4EhfPNpFR%2F2yt3umVzUyJg3GllTZxVemB34MfsL3NgqaF0GW%2FO0Tk4kRsO0CxtJPLJIh0s0EI9U3jDTW%2FXonah0ijdQh1LmP0PHBUtq6EakyMg25h01X89su%2BvOudat9BOwe0rpbJEpByRaADptuIzAK1hS2Hb02%2B4NOewC0biyc8WCDjFgWKpaqczDE43GF99R1%2FE82u6g6qRjqvWYe%2Fbo2HszF3Dp%2BLAJL%2BGNNhvpSd5X%2BOBp%2FB382z67JU8ywglDILfpYzXlDwR08W4WoV1bNck8KoxGQ42fJz0J65HLTk%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230214T124511Z&X-Amz-SignedHeaders=host&X-Amz-Expires=43200&X-Amz-Credential=ASIAWTA5OULQSN75JIND%2F20230214%2Feu-west-3%2Fs3%2Faws4_request&X-Amz-Signature=abbc0cbb77ae80b124a089fbafee45901340c536abd62979d92d7527c91c93f0"
]

for file in s3_file_liste:
    try:
        df = extract_transform(file)
    except:
        print("une erreur est survenue lors du telechargement")
