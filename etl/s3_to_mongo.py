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
    "https://nosql-taxis.s3.eu-west-3.amazonaws.com/taxi_trips_05.csv?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEDIaCWV1LXdlc3QtMyJIMEYCIQD5M7q7wygBYgPJkljRECRKHz7DG8DaQptwp4xQReMTHgIhAPAzh2o7o71wyM7ELVfEgo%2F5BMmqH1vdPESibafKeZzAKu0CCLv%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEQAhoMNDUzMTgwODI2MzM3IgxXaK0YPWEzYGbJQTkqwQLSjxfBmpObD9tBE447f0Hn6blr7LmVeA9oHUxNAEA%2Fmu53k582uiegn%2FcvAl0w9hxcTlPR5MupIG6I%2F2OJeEaoYC%2BTca4F1FOk8udYJLFdghT7MDQeTKkrHJ8oPMVK4K%2FADdzuvNc5TK7Cjx6WDJIRkh702ue1nD2T3HZhXT%2BhRyK6HnAO%2BHvxPrGIp7AV20hbNoYlBGmb5aEK3AEFnCX7z1sgqjw490kTrMxjz1yhAEhnPLCytamCVdUIZXaD0aKa28BhZFTS8SWCUWj35afFcYzTavlIn1YShS7macywzA8V6JNOiYtB%2FE5L3n9u3vOzQ0sjMi5J0dqRgkyi86O1nSKpZIFU2i2YOTJvzQQpB1gNBFYzaJM75pVw%2BY3Wx8bvphEVvK%2FQK4HT%2F3fx93IwKMygbEf288QSc33V12CBCRIw6cOdnwY6sgKbhxaZsDFLvTyr%2F8SaM%2FDMP%2BnDcrJsaD2koWh2tJML9VMXIuDgi5Hgh4UvaJpb0f%2FpIi%2FM1EJsSz3QIFdCgrW%2FDTZZgf4rjORFNxHKkQf7uiE4LP7iswgdAA1Bu1etRxPQGbRtMJEf%2F6JhiemDiVudMUfyiX2GdFY1OopoLLvecUWD0brxXjMVK1WS8dRK15YdSHrVMCbVdGw1QfNyRMimj8qOQDZe%2FxH%2BUVs1U%2FeR1DXmCZQYCIWfSl2s%2Fn%2FgxkYpuC0AFkkynmmrbfRpbafAGeLikFEGqpKYznCLaLJ6%2FmujGHRFQrW%2F9E2Mkuz3bPxm%2BbUVdAXiFmr7Wq91N5bAhBHgM7BgPLJIxyajBV5SPIB4Pxv8KUf%2B0FHOa99imU%2BOi2NrgY0Wyo6eGIscApPifiw%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230211T093931Z&X-Amz-SignedHeaders=host&X-Amz-Expires=43200&X-Amz-Credential=ASIAWTA5OULQ3FQHBAFJ%2F20230211%2Feu-west-3%2Fs3%2Faws4_request&X-Amz-Signature=6ea7944be6882ef4832a1c21e355d229c57d6c21f11d622189ff027f757f2db5",
    "https://nosql-taxis.s3.eu-west-3.amazonaws.com/taxi_trips_07.csv?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEDIaCWV1LXdlc3QtMyJIMEYCIQD5M7q7wygBYgPJkljRECRKHz7DG8DaQptwp4xQReMTHgIhAPAzh2o7o71wyM7ELVfEgo%2F5BMmqH1vdPESibafKeZzAKu0CCLv%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEQAhoMNDUzMTgwODI2MzM3IgxXaK0YPWEzYGbJQTkqwQLSjxfBmpObD9tBE447f0Hn6blr7LmVeA9oHUxNAEA%2Fmu53k582uiegn%2FcvAl0w9hxcTlPR5MupIG6I%2F2OJeEaoYC%2BTca4F1FOk8udYJLFdghT7MDQeTKkrHJ8oPMVK4K%2FADdzuvNc5TK7Cjx6WDJIRkh702ue1nD2T3HZhXT%2BhRyK6HnAO%2BHvxPrGIp7AV20hbNoYlBGmb5aEK3AEFnCX7z1sgqjw490kTrMxjz1yhAEhnPLCytamCVdUIZXaD0aKa28BhZFTS8SWCUWj35afFcYzTavlIn1YShS7macywzA8V6JNOiYtB%2FE5L3n9u3vOzQ0sjMi5J0dqRgkyi86O1nSKpZIFU2i2YOTJvzQQpB1gNBFYzaJM75pVw%2BY3Wx8bvphEVvK%2FQK4HT%2F3fx93IwKMygbEf288QSc33V12CBCRIw6cOdnwY6sgKbhxaZsDFLvTyr%2F8SaM%2FDMP%2BnDcrJsaD2koWh2tJML9VMXIuDgi5Hgh4UvaJpb0f%2FpIi%2FM1EJsSz3QIFdCgrW%2FDTZZgf4rjORFNxHKkQf7uiE4LP7iswgdAA1Bu1etRxPQGbRtMJEf%2F6JhiemDiVudMUfyiX2GdFY1OopoLLvecUWD0brxXjMVK1WS8dRK15YdSHrVMCbVdGw1QfNyRMimj8qOQDZe%2FxH%2BUVs1U%2FeR1DXmCZQYCIWfSl2s%2Fn%2FgxkYpuC0AFkkynmmrbfRpbafAGeLikFEGqpKYznCLaLJ6%2FmujGHRFQrW%2F9E2Mkuz3bPxm%2BbUVdAXiFmr7Wq91N5bAhBHgM7BgPLJIxyajBV5SPIB4Pxv8KUf%2B0FHOa99imU%2BOi2NrgY0Wyo6eGIscApPifiw%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230211T094210Z&X-Amz-SignedHeaders=host&X-Amz-Expires=43200&X-Amz-Credential=ASIAWTA5OULQ3FQHBAFJ%2F20230211%2Feu-west-3%2Fs3%2Faws4_request&X-Amz-Signature=a15eed0211bb9f2b1b563aa6f1c6bb335cf95f9654d54c583c7404dfd23892ad",
    "https://nosql-taxis.s3.eu-west-3.amazonaws.com/taxi_trips_06.csv?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEDIaCWV1LXdlc3QtMyJIMEYCIQD5M7q7wygBYgPJkljRECRKHz7DG8DaQptwp4xQReMTHgIhAPAzh2o7o71wyM7ELVfEgo%2F5BMmqH1vdPESibafKeZzAKu0CCLv%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEQAhoMNDUzMTgwODI2MzM3IgxXaK0YPWEzYGbJQTkqwQLSjxfBmpObD9tBE447f0Hn6blr7LmVeA9oHUxNAEA%2Fmu53k582uiegn%2FcvAl0w9hxcTlPR5MupIG6I%2F2OJeEaoYC%2BTca4F1FOk8udYJLFdghT7MDQeTKkrHJ8oPMVK4K%2FADdzuvNc5TK7Cjx6WDJIRkh702ue1nD2T3HZhXT%2BhRyK6HnAO%2BHvxPrGIp7AV20hbNoYlBGmb5aEK3AEFnCX7z1sgqjw490kTrMxjz1yhAEhnPLCytamCVdUIZXaD0aKa28BhZFTS8SWCUWj35afFcYzTavlIn1YShS7macywzA8V6JNOiYtB%2FE5L3n9u3vOzQ0sjMi5J0dqRgkyi86O1nSKpZIFU2i2YOTJvzQQpB1gNBFYzaJM75pVw%2BY3Wx8bvphEVvK%2FQK4HT%2F3fx93IwKMygbEf288QSc33V12CBCRIw6cOdnwY6sgKbhxaZsDFLvTyr%2F8SaM%2FDMP%2BnDcrJsaD2koWh2tJML9VMXIuDgi5Hgh4UvaJpb0f%2FpIi%2FM1EJsSz3QIFdCgrW%2FDTZZgf4rjORFNxHKkQf7uiE4LP7iswgdAA1Bu1etRxPQGbRtMJEf%2F6JhiemDiVudMUfyiX2GdFY1OopoLLvecUWD0brxXjMVK1WS8dRK15YdSHrVMCbVdGw1QfNyRMimj8qOQDZe%2FxH%2BUVs1U%2FeR1DXmCZQYCIWfSl2s%2Fn%2FgxkYpuC0AFkkynmmrbfRpbafAGeLikFEGqpKYznCLaLJ6%2FmujGHRFQrW%2F9E2Mkuz3bPxm%2BbUVdAXiFmr7Wq91N5bAhBHgM7BgPLJIxyajBV5SPIB4Pxv8KUf%2B0FHOa99imU%2BOi2NrgY0Wyo6eGIscApPifiw%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230211T094229Z&X-Amz-SignedHeaders=host&X-Amz-Expires=43200&X-Amz-Credential=ASIAWTA5OULQ3FQHBAFJ%2F20230211%2Feu-west-3%2Fs3%2Faws4_request&X-Amz-Signature=bfedac185fc2deb16086a91333ddd4065155c43507daf8c6eb9fda7e9af880b1"
]


for file in s3_file_liste:
    try:
        df = extract_transform(file)
    except:
        print("une erreur est survenue lors du telechargement")
