from pyspark.sql import SparkSession

spark = SparkSession.builder.master("spark://spark-master:7077").appName("NoSQL ETL").getOrCreate()
sc = spark.sparkContext


# Chargement des dataframes dans Mongo
def load(dataframe):
    # TODO
    return {"good": len(dataframe), "errors": 0}

# Téléchargement des fichiers de S3
def extract_transform(file_url):

    import httpx
    import pandas as pd
    from io import StringIO

    # limite la taille de telechargement pour le tests
    truncate_after = 100 * 1024 * 1024

    with httpx.stream("GET", file_url) as response:
        body = ""
        for chunk in response.iter_bytes():
            body += str(chunk)
            if response.num_bytes_downloaded >= truncate_after:
                break
    
    # bug d'encodage -> on remplace les sauts de ligne par un autre char
    body = body.replace("\\n", ";")
    df = pd.read_csv(StringIO(body), lineterminator=";")

    return df

# liste des liens des partage des fichiers sur le S3
s3_file_liste = [
    "", "", ""
]

files_paralleles = sc.parallelize(s3_file_liste)
dataframes = files_paralleles.map(extract_transform)
load_result = dataframes.map(load).collect()