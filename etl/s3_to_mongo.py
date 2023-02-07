import pymongo
import httpx
import pandas as pd
from io import StringIO
from datetime import datetime

    
myclient = pymongo.MongoClient("mongodb://192.168.3.160:27019/")
mydb = myclient["projet_nosql"]
mytable = mydb["taxis"]

mytable.delete_many({})
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
            if "2018" in dt or "2019" in dt or "2020" in dt or True:
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
                
             if len(dictionnary_list) % 1500 == 0:
                 mytable.insert_many(dictionnary_list)
                 print("insertio de ", len(dictionnary_list), "lignes")
                 dictionnary_list = []
        except:
            errors += 1
            
    print("insertion de : ", len(dictionnary_list))     
    mytable.insert_many(dictionnary_list)     
    print("done")
    
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
    "https://nosql-taxis.s3.eu-west-3.amazonaws.com/taxi_trips_00.csv?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjENL%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCWV1LXdlc3QtMyJHMEUCIQDUZIEROM1D9imb5prchSQN4ZErgNCk69B6IOKWdFpttwIga3a9uPSwtksLNtPBOsp7GO1I1vsH9Ot4mMDD7%2FX1Xk4q5AIIWxACGgw0NTMxODA4MjYzMzciDJSvrQXsihiOoK0B1yrBAi6V7ml7lumH2pA6pM3p1p4cuuhlwv5dVhXIJOOM%2FaPlS0Kg7hjUzgnTHwf7jG553e2P%2FyFiJJNPwP3saL%2BfLQEXYAAtE6ptvSk9SGpLxwq%2F6pPPrDJVgnuuasdnsdZWtaACI7gxf8cGuE%2FfygtM7iObkEAGpkQj8IDPPh2AqwqOnILEqA255OaPQeAqchqN9a4Nktx6FdgjJOIqArnb9NwmRYUvwe%2BFW1iIRZVSPJ0SxVVrjBvwJC3de9BohYVtMT9RJXPBOEKIVDJXDBHVvq7o94py6sVQfBqmot%2FYhS2qXg%2ByTl27shmRsYBehPbljrnru2dSeaJBD2Kzo%2FOzJU4DaB%2BxY1Sbox%2BHpjdNa%2Bwy1xZdqJToDMLZZPrDprWPR6mcIu71GJh%2BfQ5Hjm2%2BNsEm6wSOQ9SiZmRm7GBCl9222DDIw4ifBjqzAoTJAi6bHnCiDDhOsdvGr4iJs4Uga9umcQZ8LUrgdViBQyjtZDUER%2Bz1bx0VheWnZDrfWeN9unBGwO2suIx9fP6WwEodQWIJI2aFq9lUBf2Q%2FxrbQng1%2FW0ykFUtPctcCdszf%2BUkoq6rT1uGDvK1yQPLZSFh1QWvLe6UV6LgUtJEcoxT3k3S6BHBiz7plbLKyPXsNZSxMz1mPfn21WOKA85suquM4Oo%2Ba%2BBct1U4%2ByeIU2%2BZa5NFW9F6Ypvu7zDDtY4efsxEZo%2BdXmuDTvAmYTnry%2B%2B6AkpPqD9Hyk7hnJxXA4StfzyL8%2FzzpnVO%2B7S4lPGQOIIVCfUUphR8oZ4uwYQZdnS%2BVRiR3iaXuOZDSODM1Rt3R5I4bXrFncHFAGlvnRfLpdMF5F6dNSignS4WSiKsnVY%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230207T100427Z&X-Amz-SignedHeaders=host&X-Amz-Expires=43200&X-Amz-Credential=ASIAWTA5OULQR2QFIQB3%2F20230207%2Feu-west-3%2Fs3%2Faws4_request&X-Amz-Signature=05a5dd0208dfe2992fd7d0376cbe05c477c0d728824037db6dd08d73c3270548",
    "https://nosql-taxis.s3.eu-west-3.amazonaws.com/taxi_trips_01.csv?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjENL%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCWV1LXdlc3QtMyJHMEUCIQDUZIEROM1D9imb5prchSQN4ZErgNCk69B6IOKWdFpttwIga3a9uPSwtksLNtPBOsp7GO1I1vsH9Ot4mMDD7%2FX1Xk4q5AIIWxACGgw0NTMxODA4MjYzMzciDJSvrQXsihiOoK0B1yrBAi6V7ml7lumH2pA6pM3p1p4cuuhlwv5dVhXIJOOM%2FaPlS0Kg7hjUzgnTHwf7jG553e2P%2FyFiJJNPwP3saL%2BfLQEXYAAtE6ptvSk9SGpLxwq%2F6pPPrDJVgnuuasdnsdZWtaACI7gxf8cGuE%2FfygtM7iObkEAGpkQj8IDPPh2AqwqOnILEqA255OaPQeAqchqN9a4Nktx6FdgjJOIqArnb9NwmRYUvwe%2BFW1iIRZVSPJ0SxVVrjBvwJC3de9BohYVtMT9RJXPBOEKIVDJXDBHVvq7o94py6sVQfBqmot%2FYhS2qXg%2ByTl27shmRsYBehPbljrnru2dSeaJBD2Kzo%2FOzJU4DaB%2BxY1Sbox%2BHpjdNa%2Bwy1xZdqJToDMLZZPrDprWPR6mcIu71GJh%2BfQ5Hjm2%2BNsEm6wSOQ9SiZmRm7GBCl9222DDIw4ifBjqzAoTJAi6bHnCiDDhOsdvGr4iJs4Uga9umcQZ8LUrgdViBQyjtZDUER%2Bz1bx0VheWnZDrfWeN9unBGwO2suIx9fP6WwEodQWIJI2aFq9lUBf2Q%2FxrbQng1%2FW0ykFUtPctcCdszf%2BUkoq6rT1uGDvK1yQPLZSFh1QWvLe6UV6LgUtJEcoxT3k3S6BHBiz7plbLKyPXsNZSxMz1mPfn21WOKA85suquM4Oo%2Ba%2BBct1U4%2ByeIU2%2BZa5NFW9F6Ypvu7zDDtY4efsxEZo%2BdXmuDTvAmYTnry%2B%2B6AkpPqD9Hyk7hnJxXA4StfzyL8%2FzzpnVO%2B7S4lPGQOIIVCfUUphR8oZ4uwYQZdnS%2BVRiR3iaXuOZDSODM1Rt3R5I4bXrFncHFAGlvnRfLpdMF5F6dNSignS4WSiKsnVY%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230207T100449Z&X-Amz-SignedHeaders=host&X-Amz-Expires=43200&X-Amz-Credential=ASIAWTA5OULQR2QFIQB3%2F20230207%2Feu-west-3%2Fs3%2Faws4_request&X-Amz-Signature=3e17550a9e3ca59bbead1b1d27d6de9227ca783e6acb3f0b328e2ef782b509a7",
    "https://nosql-taxis.s3.eu-west-3.amazonaws.com/taxi_trips_02.csv?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjENL%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCWV1LXdlc3QtMyJHMEUCIQDUZIEROM1D9imb5prchSQN4ZErgNCk69B6IOKWdFpttwIga3a9uPSwtksLNtPBOsp7GO1I1vsH9Ot4mMDD7%2FX1Xk4q5AIIWxACGgw0NTMxODA4MjYzMzciDJSvrQXsihiOoK0B1yrBAi6V7ml7lumH2pA6pM3p1p4cuuhlwv5dVhXIJOOM%2FaPlS0Kg7hjUzgnTHwf7jG553e2P%2FyFiJJNPwP3saL%2BfLQEXYAAtE6ptvSk9SGpLxwq%2F6pPPrDJVgnuuasdnsdZWtaACI7gxf8cGuE%2FfygtM7iObkEAGpkQj8IDPPh2AqwqOnILEqA255OaPQeAqchqN9a4Nktx6FdgjJOIqArnb9NwmRYUvwe%2BFW1iIRZVSPJ0SxVVrjBvwJC3de9BohYVtMT9RJXPBOEKIVDJXDBHVvq7o94py6sVQfBqmot%2FYhS2qXg%2ByTl27shmRsYBehPbljrnru2dSeaJBD2Kzo%2FOzJU4DaB%2BxY1Sbox%2BHpjdNa%2Bwy1xZdqJToDMLZZPrDprWPR6mcIu71GJh%2BfQ5Hjm2%2BNsEm6wSOQ9SiZmRm7GBCl9222DDIw4ifBjqzAoTJAi6bHnCiDDhOsdvGr4iJs4Uga9umcQZ8LUrgdViBQyjtZDUER%2Bz1bx0VheWnZDrfWeN9unBGwO2suIx9fP6WwEodQWIJI2aFq9lUBf2Q%2FxrbQng1%2FW0ykFUtPctcCdszf%2BUkoq6rT1uGDvK1yQPLZSFh1QWvLe6UV6LgUtJEcoxT3k3S6BHBiz7plbLKyPXsNZSxMz1mPfn21WOKA85suquM4Oo%2Ba%2BBct1U4%2ByeIU2%2BZa5NFW9F6Ypvu7zDDtY4efsxEZo%2BdXmuDTvAmYTnry%2B%2B6AkpPqD9Hyk7hnJxXA4StfzyL8%2FzzpnVO%2B7S4lPGQOIIVCfUUphR8oZ4uwYQZdnS%2BVRiR3iaXuOZDSODM1Rt3R5I4bXrFncHFAGlvnRfLpdMF5F6dNSignS4WSiKsnVY%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230207T100504Z&X-Amz-SignedHeaders=host&X-Amz-Expires=43200&X-Amz-Credential=ASIAWTA5OULQR2QFIQB3%2F20230207%2Feu-west-3%2Fs3%2Faws4_request&X-Amz-Signature=5c49cc164ba86296b3c2e046df916d2d15824bd1ce399141bbadfa3c13f4afdc",
    "https://nosql-taxis.s3.eu-west-3.amazonaws.com/taxi_trips_03.csv?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjENL%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCWV1LXdlc3QtMyJHMEUCIQDUZIEROM1D9imb5prchSQN4ZErgNCk69B6IOKWdFpttwIga3a9uPSwtksLNtPBOsp7GO1I1vsH9Ot4mMDD7%2FX1Xk4q5AIIWxACGgw0NTMxODA4MjYzMzciDJSvrQXsihiOoK0B1yrBAi6V7ml7lumH2pA6pM3p1p4cuuhlwv5dVhXIJOOM%2FaPlS0Kg7hjUzgnTHwf7jG553e2P%2FyFiJJNPwP3saL%2BfLQEXYAAtE6ptvSk9SGpLxwq%2F6pPPrDJVgnuuasdnsdZWtaACI7gxf8cGuE%2FfygtM7iObkEAGpkQj8IDPPh2AqwqOnILEqA255OaPQeAqchqN9a4Nktx6FdgjJOIqArnb9NwmRYUvwe%2BFW1iIRZVSPJ0SxVVrjBvwJC3de9BohYVtMT9RJXPBOEKIVDJXDBHVvq7o94py6sVQfBqmot%2FYhS2qXg%2ByTl27shmRsYBehPbljrnru2dSeaJBD2Kzo%2FOzJU4DaB%2BxY1Sbox%2BHpjdNa%2Bwy1xZdqJToDMLZZPrDprWPR6mcIu71GJh%2BfQ5Hjm2%2BNsEm6wSOQ9SiZmRm7GBCl9222DDIw4ifBjqzAoTJAi6bHnCiDDhOsdvGr4iJs4Uga9umcQZ8LUrgdViBQyjtZDUER%2Bz1bx0VheWnZDrfWeN9unBGwO2suIx9fP6WwEodQWIJI2aFq9lUBf2Q%2FxrbQng1%2FW0ykFUtPctcCdszf%2BUkoq6rT1uGDvK1yQPLZSFh1QWvLe6UV6LgUtJEcoxT3k3S6BHBiz7plbLKyPXsNZSxMz1mPfn21WOKA85suquM4Oo%2Ba%2BBct1U4%2ByeIU2%2BZa5NFW9F6Ypvu7zDDtY4efsxEZo%2BdXmuDTvAmYTnry%2B%2B6AkpPqD9Hyk7hnJxXA4StfzyL8%2FzzpnVO%2B7S4lPGQOIIVCfUUphR8oZ4uwYQZdnS%2BVRiR3iaXuOZDSODM1Rt3R5I4bXrFncHFAGlvnRfLpdMF5F6dNSignS4WSiKsnVY%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230207T100516Z&X-Amz-SignedHeaders=host&X-Amz-Expires=43200&X-Amz-Credential=ASIAWTA5OULQR2QFIQB3%2F20230207%2Feu-west-3%2Fs3%2Faws4_request&X-Amz-Signature=0ba49fae04bbb33e58b8b400966a7df5e5cc74097b43c27665f58f575d123220",
    "https://nosql-taxis.s3.eu-west-3.amazonaws.com/taxi_trips_04.csv?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjENL%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCWV1LXdlc3QtMyJHMEUCIQDUZIEROM1D9imb5prchSQN4ZErgNCk69B6IOKWdFpttwIga3a9uPSwtksLNtPBOsp7GO1I1vsH9Ot4mMDD7%2FX1Xk4q5AIIWxACGgw0NTMxODA4MjYzMzciDJSvrQXsihiOoK0B1yrBAi6V7ml7lumH2pA6pM3p1p4cuuhlwv5dVhXIJOOM%2FaPlS0Kg7hjUzgnTHwf7jG553e2P%2FyFiJJNPwP3saL%2BfLQEXYAAtE6ptvSk9SGpLxwq%2F6pPPrDJVgnuuasdnsdZWtaACI7gxf8cGuE%2FfygtM7iObkEAGpkQj8IDPPh2AqwqOnILEqA255OaPQeAqchqN9a4Nktx6FdgjJOIqArnb9NwmRYUvwe%2BFW1iIRZVSPJ0SxVVrjBvwJC3de9BohYVtMT9RJXPBOEKIVDJXDBHVvq7o94py6sVQfBqmot%2FYhS2qXg%2ByTl27shmRsYBehPbljrnru2dSeaJBD2Kzo%2FOzJU4DaB%2BxY1Sbox%2BHpjdNa%2Bwy1xZdqJToDMLZZPrDprWPR6mcIu71GJh%2BfQ5Hjm2%2BNsEm6wSOQ9SiZmRm7GBCl9222DDIw4ifBjqzAoTJAi6bHnCiDDhOsdvGr4iJs4Uga9umcQZ8LUrgdViBQyjtZDUER%2Bz1bx0VheWnZDrfWeN9unBGwO2suIx9fP6WwEodQWIJI2aFq9lUBf2Q%2FxrbQng1%2FW0ykFUtPctcCdszf%2BUkoq6rT1uGDvK1yQPLZSFh1QWvLe6UV6LgUtJEcoxT3k3S6BHBiz7plbLKyPXsNZSxMz1mPfn21WOKA85suquM4Oo%2Ba%2BBct1U4%2ByeIU2%2BZa5NFW9F6Ypvu7zDDtY4efsxEZo%2BdXmuDTvAmYTnry%2B%2B6AkpPqD9Hyk7hnJxXA4StfzyL8%2FzzpnVO%2B7S4lPGQOIIVCfUUphR8oZ4uwYQZdnS%2BVRiR3iaXuOZDSODM1Rt3R5I4bXrFncHFAGlvnRfLpdMF5F6dNSignS4WSiKsnVY%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230207T100528Z&X-Amz-SignedHeaders=host&X-Amz-Expires=43200&X-Amz-Credential=ASIAWTA5OULQR2QFIQB3%2F20230207%2Feu-west-3%2Fs3%2Faws4_request&X-Amz-Signature=0119e1a55b840175c50be55106c5db9abae055c2d3f586b871709c36cf346890",
    "https://nosql-taxis.s3.eu-west-3.amazonaws.com/taxi_trips_05.csv?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjENL%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCWV1LXdlc3QtMyJHMEUCIQDUZIEROM1D9imb5prchSQN4ZErgNCk69B6IOKWdFpttwIga3a9uPSwtksLNtPBOsp7GO1I1vsH9Ot4mMDD7%2FX1Xk4q5AIIWxACGgw0NTMxODA4MjYzMzciDJSvrQXsihiOoK0B1yrBAi6V7ml7lumH2pA6pM3p1p4cuuhlwv5dVhXIJOOM%2FaPlS0Kg7hjUzgnTHwf7jG553e2P%2FyFiJJNPwP3saL%2BfLQEXYAAtE6ptvSk9SGpLxwq%2F6pPPrDJVgnuuasdnsdZWtaACI7gxf8cGuE%2FfygtM7iObkEAGpkQj8IDPPh2AqwqOnILEqA255OaPQeAqchqN9a4Nktx6FdgjJOIqArnb9NwmRYUvwe%2BFW1iIRZVSPJ0SxVVrjBvwJC3de9BohYVtMT9RJXPBOEKIVDJXDBHVvq7o94py6sVQfBqmot%2FYhS2qXg%2ByTl27shmRsYBehPbljrnru2dSeaJBD2Kzo%2FOzJU4DaB%2BxY1Sbox%2BHpjdNa%2Bwy1xZdqJToDMLZZPrDprWPR6mcIu71GJh%2BfQ5Hjm2%2BNsEm6wSOQ9SiZmRm7GBCl9222DDIw4ifBjqzAoTJAi6bHnCiDDhOsdvGr4iJs4Uga9umcQZ8LUrgdViBQyjtZDUER%2Bz1bx0VheWnZDrfWeN9unBGwO2suIx9fP6WwEodQWIJI2aFq9lUBf2Q%2FxrbQng1%2FW0ykFUtPctcCdszf%2BUkoq6rT1uGDvK1yQPLZSFh1QWvLe6UV6LgUtJEcoxT3k3S6BHBiz7plbLKyPXsNZSxMz1mPfn21WOKA85suquM4Oo%2Ba%2BBct1U4%2ByeIU2%2BZa5NFW9F6Ypvu7zDDtY4efsxEZo%2BdXmuDTvAmYTnry%2B%2B6AkpPqD9Hyk7hnJxXA4StfzyL8%2FzzpnVO%2B7S4lPGQOIIVCfUUphR8oZ4uwYQZdnS%2BVRiR3iaXuOZDSODM1Rt3R5I4bXrFncHFAGlvnRfLpdMF5F6dNSignS4WSiKsnVY%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230207T100549Z&X-Amz-SignedHeaders=host&X-Amz-Expires=43200&X-Amz-Credential=ASIAWTA5OULQR2QFIQB3%2F20230207%2Feu-west-3%2Fs3%2Faws4_request&X-Amz-Signature=a47b345797acfe89b2742cb01e1b85ddeddff7b74577b63c7916076109cce2a1",
    "https://nosql-taxis.s3.eu-west-3.amazonaws.com/taxi_trips_06.csv?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjENL%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCWV1LXdlc3QtMyJHMEUCIQDUZIEROM1D9imb5prchSQN4ZErgNCk69B6IOKWdFpttwIga3a9uPSwtksLNtPBOsp7GO1I1vsH9Ot4mMDD7%2FX1Xk4q5AIIWxACGgw0NTMxODA4MjYzMzciDJSvrQXsihiOoK0B1yrBAi6V7ml7lumH2pA6pM3p1p4cuuhlwv5dVhXIJOOM%2FaPlS0Kg7hjUzgnTHwf7jG553e2P%2FyFiJJNPwP3saL%2BfLQEXYAAtE6ptvSk9SGpLxwq%2F6pPPrDJVgnuuasdnsdZWtaACI7gxf8cGuE%2FfygtM7iObkEAGpkQj8IDPPh2AqwqOnILEqA255OaPQeAqchqN9a4Nktx6FdgjJOIqArnb9NwmRYUvwe%2BFW1iIRZVSPJ0SxVVrjBvwJC3de9BohYVtMT9RJXPBOEKIVDJXDBHVvq7o94py6sVQfBqmot%2FYhS2qXg%2ByTl27shmRsYBehPbljrnru2dSeaJBD2Kzo%2FOzJU4DaB%2BxY1Sbox%2BHpjdNa%2Bwy1xZdqJToDMLZZPrDprWPR6mcIu71GJh%2BfQ5Hjm2%2BNsEm6wSOQ9SiZmRm7GBCl9222DDIw4ifBjqzAoTJAi6bHnCiDDhOsdvGr4iJs4Uga9umcQZ8LUrgdViBQyjtZDUER%2Bz1bx0VheWnZDrfWeN9unBGwO2suIx9fP6WwEodQWIJI2aFq9lUBf2Q%2FxrbQng1%2FW0ykFUtPctcCdszf%2BUkoq6rT1uGDvK1yQPLZSFh1QWvLe6UV6LgUtJEcoxT3k3S6BHBiz7plbLKyPXsNZSxMz1mPfn21WOKA85suquM4Oo%2Ba%2BBct1U4%2ByeIU2%2BZa5NFW9F6Ypvu7zDDtY4efsxEZo%2BdXmuDTvAmYTnry%2B%2B6AkpPqD9Hyk7hnJxXA4StfzyL8%2FzzpnVO%2B7S4lPGQOIIVCfUUphR8oZ4uwYQZdnS%2BVRiR3iaXuOZDSODM1Rt3R5I4bXrFncHFAGlvnRfLpdMF5F6dNSignS4WSiKsnVY%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230207T100603Z&X-Amz-SignedHeaders=host&X-Amz-Expires=43200&X-Amz-Credential=ASIAWTA5OULQR2QFIQB3%2F20230207%2Feu-west-3%2Fs3%2Faws4_request&X-Amz-Signature=34c58f0f1e460f12ad13a7a10881e553b386c343b6473d7a8238375ca9553e8e",
    "https://nosql-taxis.s3.eu-west-3.amazonaws.com/taxi_trips_07.csv?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjENL%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCWV1LXdlc3QtMyJHMEUCIQDUZIEROM1D9imb5prchSQN4ZErgNCk69B6IOKWdFpttwIga3a9uPSwtksLNtPBOsp7GO1I1vsH9Ot4mMDD7%2FX1Xk4q5AIIWxACGgw0NTMxODA4MjYzMzciDJSvrQXsihiOoK0B1yrBAi6V7ml7lumH2pA6pM3p1p4cuuhlwv5dVhXIJOOM%2FaPlS0Kg7hjUzgnTHwf7jG553e2P%2FyFiJJNPwP3saL%2BfLQEXYAAtE6ptvSk9SGpLxwq%2F6pPPrDJVgnuuasdnsdZWtaACI7gxf8cGuE%2FfygtM7iObkEAGpkQj8IDPPh2AqwqOnILEqA255OaPQeAqchqN9a4Nktx6FdgjJOIqArnb9NwmRYUvwe%2BFW1iIRZVSPJ0SxVVrjBvwJC3de9BohYVtMT9RJXPBOEKIVDJXDBHVvq7o94py6sVQfBqmot%2FYhS2qXg%2ByTl27shmRsYBehPbljrnru2dSeaJBD2Kzo%2FOzJU4DaB%2BxY1Sbox%2BHpjdNa%2Bwy1xZdqJToDMLZZPrDprWPR6mcIu71GJh%2BfQ5Hjm2%2BNsEm6wSOQ9SiZmRm7GBCl9222DDIw4ifBjqzAoTJAi6bHnCiDDhOsdvGr4iJs4Uga9umcQZ8LUrgdViBQyjtZDUER%2Bz1bx0VheWnZDrfWeN9unBGwO2suIx9fP6WwEodQWIJI2aFq9lUBf2Q%2FxrbQng1%2FW0ykFUtPctcCdszf%2BUkoq6rT1uGDvK1yQPLZSFh1QWvLe6UV6LgUtJEcoxT3k3S6BHBiz7plbLKyPXsNZSxMz1mPfn21WOKA85suquM4Oo%2Ba%2BBct1U4%2ByeIU2%2BZa5NFW9F6Ypvu7zDDtY4efsxEZo%2BdXmuDTvAmYTnry%2B%2B6AkpPqD9Hyk7hnJxXA4StfzyL8%2FzzpnVO%2B7S4lPGQOIIVCfUUphR8oZ4uwYQZdnS%2BVRiR3iaXuOZDSODM1Rt3R5I4bXrFncHFAGlvnRfLpdMF5F6dNSignS4WSiKsnVY%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230207T100615Z&X-Amz-SignedHeaders=host&X-Amz-Expires=43200&X-Amz-Credential=ASIAWTA5OULQR2QFIQB3%2F20230207%2Feu-west-3%2Fs3%2Faws4_request&X-Amz-Signature=2470f27a857b7adbc12b4e47c9d5fb01e39743b3b3400d6dd9b5a9fa9554510a"
    
]


for file in s3_file_liste:
    try:
        df = extract_transform(file)
    except:
        print("une erreur est survenue lors du telechargement")
