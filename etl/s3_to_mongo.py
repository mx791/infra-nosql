import pymongo
import httpx
import pandas as pd
from io import StringIO
from datetime import datetime

    
myclient = pymongo.MongoClient("mongodb://192.168.3.160:27019/")
mydb = myclient["projet_nosql"]
mytable = mydb["taxis"]

mytable.delete_many({})

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
            if "2018" in dt or "2019" in dt or "2020" in dt:
                dict = {}
                for col in columns:
                    dict[col] = dataframe[col][id]
                        
                dictionnary_list.append(dict)
                loaded += 1
        except:
            errors += 1
            
        if len(dictionnary_list) > 25000 or len(dataframe) == id + 1:
            mytable.insert_many(dictionnary_list)
            dictionnary_list = []
            print("chargement de", loaded, "lignes")        
    
    return {"good": len(dictionnary_list), "errors": errors}

# Téléchargement des fichiers de S3
def extract_transform(file_url):

    print("Telechargement du fichier")

    # limite la taille de telechargement pour le tests
    truncate_after = int(650  * 1024 * 1024)

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
    "https://nosql-taxis.s3.eu-west-3.amazonaws.com/taxi_trips_00.csv?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjELn%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCWV1LXdlc3QtMyJHMEUCIHLOBbBZMppJeBTRCIAhTMCUkRUOzi0g6aERvEMeGhP4AiEAnuEyrVDNKUd72CwIUiknKnlhd73ydr6wRezGTEkQAeQq5AIIQhACGgw0NTMxODA4MjYzMzciDAAXWqQt3TfWgiJBgSrBAnVj%2FuDsOE%2FcguwIHHZrIzAfECeU7RUn7%2B1yCZkrOd6rgZMPve1yYkn4R2NCmi50bf6gyGY73Fyxd0XXG5sttE5gUx43r90XkWaEVFWN5wXc8vbxyhspN2RKGdWfxBV160dUG6N2YspQXBQCYjNIZHcLf5tJGBGmfx4vX3Hxo3lGfvbq1uJ2p6b6zpOtSoTuldzq4GGMb4TJMy04YnTIdpWLhhuvQuH1KAj65m62bxS1eZl%2FbuSt5aME5w64QU33etiA7buRTmyrYRMFE1Fg2Qtv834XNcQvsRhXyS247%2FTTKmnBOycv%2FYfVa60TdVQrNoyTNpJp969tdR2ZgKo4LUUF9J6Vhaxmkq7lqUXBnp7Bx%2FkZAd4ZDX7iUEveBahvlvzDmZCZH%2BGTmvGnJv2BtDa2nep7nyqfM51zhi8ILaGkIDDh8oKfBjqzAogDuM68wAOsgru81kY%2B7xwuYF%2BL7hPGvKyOdunqmcIjfs0Qgzuu80B4pexAdOYsvi2If85nIItm0wfSehGaFl1yYDGMQ9usIzyAn84IL6PkEkKvv4JbKX1MvrCI9IuALfiTDw%2Fm9WX%2Bu2ZO%2B%2F6H1yyDPwg9KeTUHxoRgiSpbt38%2Bomh7b1TaVkAmX%2BnfE7TAFb0FOiRyt8SBj3Shqi5O8tTBJHKLeocya%2FoStPhmvimD1qf0jHCB%2F0ns1tREProA1i84Z%2BNNQF0iXcFVCMffYiPysXjJPyW91bs8ol0BeLl%2Bmbf1w1dC2WcWrzpcmsqE%2Bihild8pMVDSGeCOHizhslg%2F9tT%2BCduiQkgbeWF%2FQe8PPZTHlMCF%2FsN1%2BIawP6Q%2B9knZ9yVSgnZ%2B%2BTfZY1fepuBGR4%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230206T082521Z&X-Amz-SignedHeaders=host&X-Amz-Expires=43200&X-Amz-Credential=ASIAWTA5OULQVHNCFQEO%2F20230206%2Feu-west-3%2Fs3%2Faws4_request&X-Amz-Signature=72f462f74226106f9bbb95296a3343a35d9a43a0f412555262e06e20d8b44dfb",
    "https://nosql-taxis.s3.eu-west-3.amazonaws.com/taxi_trips_01.csv?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjELn%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCWV1LXdlc3QtMyJHMEUCIHLOBbBZMppJeBTRCIAhTMCUkRUOzi0g6aERvEMeGhP4AiEAnuEyrVDNKUd72CwIUiknKnlhd73ydr6wRezGTEkQAeQq5AIIQhACGgw0NTMxODA4MjYzMzciDAAXWqQt3TfWgiJBgSrBAnVj%2FuDsOE%2FcguwIHHZrIzAfECeU7RUn7%2B1yCZkrOd6rgZMPve1yYkn4R2NCmi50bf6gyGY73Fyxd0XXG5sttE5gUx43r90XkWaEVFWN5wXc8vbxyhspN2RKGdWfxBV160dUG6N2YspQXBQCYjNIZHcLf5tJGBGmfx4vX3Hxo3lGfvbq1uJ2p6b6zpOtSoTuldzq4GGMb4TJMy04YnTIdpWLhhuvQuH1KAj65m62bxS1eZl%2FbuSt5aME5w64QU33etiA7buRTmyrYRMFE1Fg2Qtv834XNcQvsRhXyS247%2FTTKmnBOycv%2FYfVa60TdVQrNoyTNpJp969tdR2ZgKo4LUUF9J6Vhaxmkq7lqUXBnp7Bx%2FkZAd4ZDX7iUEveBahvlvzDmZCZH%2BGTmvGnJv2BtDa2nep7nyqfM51zhi8ILaGkIDDh8oKfBjqzAogDuM68wAOsgru81kY%2B7xwuYF%2BL7hPGvKyOdunqmcIjfs0Qgzuu80B4pexAdOYsvi2If85nIItm0wfSehGaFl1yYDGMQ9usIzyAn84IL6PkEkKvv4JbKX1MvrCI9IuALfiTDw%2Fm9WX%2Bu2ZO%2B%2F6H1yyDPwg9KeTUHxoRgiSpbt38%2Bomh7b1TaVkAmX%2BnfE7TAFb0FOiRyt8SBj3Shqi5O8tTBJHKLeocya%2FoStPhmvimD1qf0jHCB%2F0ns1tREProA1i84Z%2BNNQF0iXcFVCMffYiPysXjJPyW91bs8ol0BeLl%2Bmbf1w1dC2WcWrzpcmsqE%2Bihild8pMVDSGeCOHizhslg%2F9tT%2BCduiQkgbeWF%2FQe8PPZTHlMCF%2FsN1%2BIawP6Q%2B9knZ9yVSgnZ%2B%2BTfZY1fepuBGR4%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230206T092143Z&X-Amz-SignedHeaders=host&X-Amz-Expires=43200&X-Amz-Credential=ASIAWTA5OULQVHNCFQEO%2F20230206%2Feu-west-3%2Fs3%2Faws4_request&X-Amz-Signature=0703d293715a00ab6087428d4a5936eb37a012cfccb58f8c2b7d0b35d39e179b"
]


for file in s3_file_liste:
    try:
        df = extract_transform(file)
    except:
        print("une erreur est survenue lors du telechargement")
