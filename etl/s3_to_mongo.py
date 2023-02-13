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
    "https://nosql-taxis.s3.eu-west-3.amazonaws.com/taxi_trips_07.csv?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEGAaCWV1LXdlc3QtMyJGMEQCIFGRq6Bs%2Fo6sirR2lu6VEwQdw8O5gfm5DLBOEbOaNyQgAiA%2BEhoAT1DaUMdG6mBgl93oyQK6brb4WcC1ydTjW%2FaDbSrtAgjp%2F%2F%2F%2F%2F%2F%2F%2F%2F%2F8BEAIaDDQ1MzE4MDgyNjMzNyIMjSD7JZx9RFIFMSg2KsECLfrqXFIjgd84FMjqlKCpbD3FC7j6Ckq3StP6PyXrxMmGdIFCmkHLIrlBJzxJxaDWXUrqsfJukvUdgLUzBsuBYewQ5IKuYJTL%2FF82mZFTaeBWdssMCwEmcIatvXaWz%2FWIYFRCOMwFBUXoyPXghsMwkIlM5xT7D7f7kDxzjAbX36Qq8yGE%2BXeb%2FdbgP4x18R7s7%2BSxTJSyG%2F4phSbDVUM6zY9NR6Vc%2Fa6mxZd9odqGoM7ew2hvYjZlSnzPMIkko%2FWwF6dLGDWg1LYPaYloGdbqWByGtMoLtQnhc3Nyg1kR1a7mriav4NKGUMett2XsxHz3e0kCcBVdK0%2FzUVpLt9axU26ce9hzIVTHC8b88%2BzmP4XNhZgPCVjmQcyJXgJv2NoMgy%2FSrriq5DW8GMwIWXH5ze8n09Zalz4GXD4FylpgZDReMLzXp58GOrQCA3blhOU0UittWYELa7oe5NonhiD6CeIUjFQtmMuk%2BMx9FRMVhZHxV9qnnpLnpGjOIJx5De8R%2FqvY0c6uFVlf%2Fi%2FGIg1tFy7m%2Bnhwck3IaUukpzEFf7hgTDKEXltR4voiSXD8jRTauk6LbVrsEo8EHc34x0LSnFsX%2B0pntIYa2uKCz%2B%2FziJqJwRkmVaH4ZTDbQmRIOlT3pDsc1l%2BThhb%2FyZJSXt8X9qz02P5sMuZK7uoLQS8R7waBcVsN7CimUPbkM0TC1cekfHWHV2htCt953oGLqn8O6LYAW5CBJVZ%2FWW6Ab4muASFCvCKUJcrqgsrqQVVuDhwZhuL5MgTG3bDzz3or8StCMFrYIAy%2FhTh1ZJPqaji6sc%2BkRSGbG18%2BX0pzzPTQbruWQ0RsljAinyhDPIHF5j0%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230213T075033Z&X-Amz-SignedHeaders=host&X-Amz-Expires=43200&X-Amz-Credential=ASIAWTA5OULQTV2APHML%2F20230213%2Feu-west-3%2Fs3%2Faws4_request&X-Amz-Signature=af8ca4a86f0ea11b45b05611221f3841081ffee80c2a97503cd37af6b373b760"]


for file in s3_file_liste:
    try:
        df = extract_transform(file)
    except:
        print("une erreur est survenue lors du telechargement")
