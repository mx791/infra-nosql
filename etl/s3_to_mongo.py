import pymongo
import httpx
import pandas as pd
from io import StringIO
from datetime import datetime


# Chargement des dataframes dans Mongo
def load(dataframe):
    
    print("load: ", len(dataframe))

    myclient = pymongo.MongoClient("mongodb://192.168.3.160:27019/")
    mydb = myclient["projet_nosql"]
    mytable = mydb["taxis"]
    
    print(list(mydb.list_collection_names()))
    
    errors = 0
    loaded = 0


    columns = dataframe.columns

    for id in range(len(dataframe)):

        dt = dataframe["Trip Start Timestamp"][id]
        dictionnary_list = []
        try:
            if "2018" in dt or "2019" in dt or "2020" in dt:
                dict = {}
                for col in columns:
                    dict[col] = dataframe[col][id]
                dictionnary_list.append(dict)
                loaded += 1
        except:
            errors += 1
        if id % 1000 == 0:
            mytable.insert_many(dictionnary_list)
            dictionnary_list = []
            print(loaded, errors)

    print(len(dictionnary_list), len(df))
        
    
    return {"good": len(dictionnary_list), "errors": errors}

# Téléchargement des fichiers de S3
def extract_transform(file_url):

    print("download...")

    # limite la taille de telechargement pour le tests
    truncate_after = 10  * 1024 * 1024

    with httpx.stream("GET", file_url) as response:
        body = ""
        for chunk in response.iter_bytes():
            body += str(chunk)
            if response.num_bytes_downloaded >= truncate_after:
                break
    
    # bug d'encodage -> on remplace les sauts de ligne par un autre char
    body = body.replace("\\n", ";")
    body = body.replace("b'", "")
    df = pd.read_csv(StringIO(body), lineterminator=";")

    return df

# liste des liens des partage des fichiers sur le S3
s3_file_liste = [
    "https://nosql-taxis.s3.eu-west-3.amazonaws.com/taxi_trips_00.csv?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEKr%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCWV1LXdlc3QtMyJHMEUCIC%2FUznqMwl9b1UEeDCzflkP%2BQOKDBxZmOX%2FIPglwOc62AiEA02cKsjDt3L6dG0Tfjgran0%2Fw1RUt6s8mNtI2KPmXme0q5AIIMhACGgw0NTMxODA4MjYzMzciDKGMEhqX47dt2ykz4irBAiuAgvQGeDTYoTfYnqLrezq83lF1j0KJEA6SP3LciRi0wsOkzx5fCDwpzwJ%2BRMMLMENMtTtgemVXxcp5yhSuz3z5g1kTRpbMQdVOZ%2BsGJ5B66k6r7NxwWjvBbKok9FUeonaZqtrlSuqoVGGRzvMnZWES0cMC8%2B8GgVv2X3h0YzHeXG8r8tVHD2z2mRxMznX%2FyIdvaTiz9ZGKwglsO6DbMVBp%2Bt4p14noN7in8ITe8%2FirSazXLGCcei2QJ0XAkam5wSoCKd7WC0M7D03LHZv%2FkIDL98MUMyY%2F89jr3XYpowgjWJMOXemm%2BJUiJEvF3YQ4OZWpiVkgZQtyJpRdF1wDIAr0wmrFSQlvlG72hQN0uSaJYw2Sy84S7pPw3LPKkhxTWS8eARvL0IwAwkx%2BVtTIb4M0cQtRWM%2FFiCWbJEM7O1%2B5sjCyyP%2BeBjqzAmZI0wG%2FJ8srx7h2HUsoKrxjJ7keZ4B6cculpXJYP4QHTszmk7afTqrQiQa4jnqeYtm5B%2BXvSKI7fDbEt9ULBUrGaxOfz1yJWVzlkwxTWkun41j8k206Sy5x8Wj8sXcCFB7R61Emt1b7oNLrtn0CCFbVCbRX28gxUj9wi9%2B2zD0zvaDmh3I8%2B1i91Kd5xqZnu8eNhL9Cb8gRt9ejg006NtrbOLZU8j3CNoXWE%2FPUVGnI8jvuOc0V%2BpC776pVlrJnSb3fGNLn8HKrlg9beGkesYZPknjvOLuZDGmfb4BrG5x0kQ%2FhDyZP1P8k2OXpCgD00yM1Sfljdr09DRC8RdsLbiS59J6YdMA2lHpFn3TR%2BeEqasXwjShtjE0lJqp4AYvcifdWXfXn9ocxvwZdueha76c5Qf0%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230205T174331Z&X-Amz-SignedHeaders=host&X-Amz-Expires=43200&X-Amz-Credential=ASIAWTA5OULQUNFPIBGM%2F20230205%2Feu-west-3%2Fs3%2Faws4_request&X-Amz-Signature=73c738705e3fb70b6c6de63a5ed8c6b09c3774d2d019f9faaa2f9bfe54207620",
    "https://nosql-taxis.s3.eu-west-3.amazonaws.com/taxi_trips_01.csv?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEKr%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCWV1LXdlc3QtMyJHMEUCIC%2FUznqMwl9b1UEeDCzflkP%2BQOKDBxZmOX%2FIPglwOc62AiEA02cKsjDt3L6dG0Tfjgran0%2Fw1RUt6s8mNtI2KPmXme0q5AIIMhACGgw0NTMxODA4MjYzMzciDKGMEhqX47dt2ykz4irBAiuAgvQGeDTYoTfYnqLrezq83lF1j0KJEA6SP3LciRi0wsOkzx5fCDwpzwJ%2BRMMLMENMtTtgemVXxcp5yhSuz3z5g1kTRpbMQdVOZ%2BsGJ5B66k6r7NxwWjvBbKok9FUeonaZqtrlSuqoVGGRzvMnZWES0cMC8%2B8GgVv2X3h0YzHeXG8r8tVHD2z2mRxMznX%2FyIdvaTiz9ZGKwglsO6DbMVBp%2Bt4p14noN7in8ITe8%2FirSazXLGCcei2QJ0XAkam5wSoCKd7WC0M7D03LHZv%2FkIDL98MUMyY%2F89jr3XYpowgjWJMOXemm%2BJUiJEvF3YQ4OZWpiVkgZQtyJpRdF1wDIAr0wmrFSQlvlG72hQN0uSaJYw2Sy84S7pPw3LPKkhxTWS8eARvL0IwAwkx%2BVtTIb4M0cQtRWM%2FFiCWbJEM7O1%2B5sjCyyP%2BeBjqzAmZI0wG%2FJ8srx7h2HUsoKrxjJ7keZ4B6cculpXJYP4QHTszmk7afTqrQiQa4jnqeYtm5B%2BXvSKI7fDbEt9ULBUrGaxOfz1yJWVzlkwxTWkun41j8k206Sy5x8Wj8sXcCFB7R61Emt1b7oNLrtn0CCFbVCbRX28gxUj9wi9%2B2zD0zvaDmh3I8%2B1i91Kd5xqZnu8eNhL9Cb8gRt9ejg006NtrbOLZU8j3CNoXWE%2FPUVGnI8jvuOc0V%2BpC776pVlrJnSb3fGNLn8HKrlg9beGkesYZPknjvOLuZDGmfb4BrG5x0kQ%2FhDyZP1P8k2OXpCgD00yM1Sfljdr09DRC8RdsLbiS59J6YdMA2lHpFn3TR%2BeEqasXwjShtjE0lJqp4AYvcifdWXfXn9ocxvwZdueha76c5Qf0%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230205T180536Z&X-Amz-SignedHeaders=host&X-Amz-Expires=719&X-Amz-Credential=ASIAWTA5OULQUNFPIBGM%2F20230205%2Feu-west-3%2Fs3%2Faws4_request&X-Amz-Signature=8bb2b460364fb420fce6cd008115b35e440afcecfe8e546049ff01d917f03599",
    "https://nosql-taxis.s3.eu-west-3.amazonaws.com/taxi_trips_02.csv?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEKr%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCWV1LXdlc3QtMyJHMEUCIC%2FUznqMwl9b1UEeDCzflkP%2BQOKDBxZmOX%2FIPglwOc62AiEA02cKsjDt3L6dG0Tfjgran0%2Fw1RUt6s8mNtI2KPmXme0q5AIIMhACGgw0NTMxODA4MjYzMzciDKGMEhqX47dt2ykz4irBAiuAgvQGeDTYoTfYnqLrezq83lF1j0KJEA6SP3LciRi0wsOkzx5fCDwpzwJ%2BRMMLMENMtTtgemVXxcp5yhSuz3z5g1kTRpbMQdVOZ%2BsGJ5B66k6r7NxwWjvBbKok9FUeonaZqtrlSuqoVGGRzvMnZWES0cMC8%2B8GgVv2X3h0YzHeXG8r8tVHD2z2mRxMznX%2FyIdvaTiz9ZGKwglsO6DbMVBp%2Bt4p14noN7in8ITe8%2FirSazXLGCcei2QJ0XAkam5wSoCKd7WC0M7D03LHZv%2FkIDL98MUMyY%2F89jr3XYpowgjWJMOXemm%2BJUiJEvF3YQ4OZWpiVkgZQtyJpRdF1wDIAr0wmrFSQlvlG72hQN0uSaJYw2Sy84S7pPw3LPKkhxTWS8eARvL0IwAwkx%2BVtTIb4M0cQtRWM%2FFiCWbJEM7O1%2B5sjCyyP%2BeBjqzAmZI0wG%2FJ8srx7h2HUsoKrxjJ7keZ4B6cculpXJYP4QHTszmk7afTqrQiQa4jnqeYtm5B%2BXvSKI7fDbEt9ULBUrGaxOfz1yJWVzlkwxTWkun41j8k206Sy5x8Wj8sXcCFB7R61Emt1b7oNLrtn0CCFbVCbRX28gxUj9wi9%2B2zD0zvaDmh3I8%2B1i91Kd5xqZnu8eNhL9Cb8gRt9ejg006NtrbOLZU8j3CNoXWE%2FPUVGnI8jvuOc0V%2BpC776pVlrJnSb3fGNLn8HKrlg9beGkesYZPknjvOLuZDGmfb4BrG5x0kQ%2FhDyZP1P8k2OXpCgD00yM1Sfljdr09DRC8RdsLbiS59J6YdMA2lHpFn3TR%2BeEqasXwjShtjE0lJqp4AYvcifdWXfXn9ocxvwZdueha76c5Qf0%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230205T180601Z&X-Amz-SignedHeaders=host&X-Amz-Expires=43200&X-Amz-Credential=ASIAWTA5OULQUNFPIBGM%2F20230205%2Feu-west-3%2Fs3%2Faws4_request&X-Amz-Signature=f12024eb33d7c4a05da58485e776ebf83c44ff842f1784115e39a8a948234f34",
    "https://nosql-taxis.s3.eu-west-3.amazonaws.com/taxi_trips_03.csv?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEKr%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCWV1LXdlc3QtMyJHMEUCIC%2FUznqMwl9b1UEeDCzflkP%2BQOKDBxZmOX%2FIPglwOc62AiEA02cKsjDt3L6dG0Tfjgran0%2Fw1RUt6s8mNtI2KPmXme0q5AIIMhACGgw0NTMxODA4MjYzMzciDKGMEhqX47dt2ykz4irBAiuAgvQGeDTYoTfYnqLrezq83lF1j0KJEA6SP3LciRi0wsOkzx5fCDwpzwJ%2BRMMLMENMtTtgemVXxcp5yhSuz3z5g1kTRpbMQdVOZ%2BsGJ5B66k6r7NxwWjvBbKok9FUeonaZqtrlSuqoVGGRzvMnZWES0cMC8%2B8GgVv2X3h0YzHeXG8r8tVHD2z2mRxMznX%2FyIdvaTiz9ZGKwglsO6DbMVBp%2Bt4p14noN7in8ITe8%2FirSazXLGCcei2QJ0XAkam5wSoCKd7WC0M7D03LHZv%2FkIDL98MUMyY%2F89jr3XYpowgjWJMOXemm%2BJUiJEvF3YQ4OZWpiVkgZQtyJpRdF1wDIAr0wmrFSQlvlG72hQN0uSaJYw2Sy84S7pPw3LPKkhxTWS8eARvL0IwAwkx%2BVtTIb4M0cQtRWM%2FFiCWbJEM7O1%2B5sjCyyP%2BeBjqzAmZI0wG%2FJ8srx7h2HUsoKrxjJ7keZ4B6cculpXJYP4QHTszmk7afTqrQiQa4jnqeYtm5B%2BXvSKI7fDbEt9ULBUrGaxOfz1yJWVzlkwxTWkun41j8k206Sy5x8Wj8sXcCFB7R61Emt1b7oNLrtn0CCFbVCbRX28gxUj9wi9%2B2zD0zvaDmh3I8%2B1i91Kd5xqZnu8eNhL9Cb8gRt9ejg006NtrbOLZU8j3CNoXWE%2FPUVGnI8jvuOc0V%2BpC776pVlrJnSb3fGNLn8HKrlg9beGkesYZPknjvOLuZDGmfb4BrG5x0kQ%2FhDyZP1P8k2OXpCgD00yM1Sfljdr09DRC8RdsLbiS59J6YdMA2lHpFn3TR%2BeEqasXwjShtjE0lJqp4AYvcifdWXfXn9ocxvwZdueha76c5Qf0%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230205T180937Z&X-Amz-SignedHeaders=host&X-Amz-Expires=43200&X-Amz-Credential=ASIAWTA5OULQUNFPIBGM%2F20230205%2Feu-west-3%2Fs3%2Faws4_request&X-Amz-Signature=776343b5a10f0f7a99bc1a95b6d343b41cd295a1da922cc885d6d43c6ff9bf4c",
    "https://nosql-taxis.s3.eu-west-3.amazonaws.com/taxi_trips_04.csv?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEKr%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCWV1LXdlc3QtMyJHMEUCIC%2FUznqMwl9b1UEeDCzflkP%2BQOKDBxZmOX%2FIPglwOc62AiEA02cKsjDt3L6dG0Tfjgran0%2Fw1RUt6s8mNtI2KPmXme0q5AIIMhACGgw0NTMxODA4MjYzMzciDKGMEhqX47dt2ykz4irBAiuAgvQGeDTYoTfYnqLrezq83lF1j0KJEA6SP3LciRi0wsOkzx5fCDwpzwJ%2BRMMLMENMtTtgemVXxcp5yhSuz3z5g1kTRpbMQdVOZ%2BsGJ5B66k6r7NxwWjvBbKok9FUeonaZqtrlSuqoVGGRzvMnZWES0cMC8%2B8GgVv2X3h0YzHeXG8r8tVHD2z2mRxMznX%2FyIdvaTiz9ZGKwglsO6DbMVBp%2Bt4p14noN7in8ITe8%2FirSazXLGCcei2QJ0XAkam5wSoCKd7WC0M7D03LHZv%2FkIDL98MUMyY%2F89jr3XYpowgjWJMOXemm%2BJUiJEvF3YQ4OZWpiVkgZQtyJpRdF1wDIAr0wmrFSQlvlG72hQN0uSaJYw2Sy84S7pPw3LPKkhxTWS8eARvL0IwAwkx%2BVtTIb4M0cQtRWM%2FFiCWbJEM7O1%2B5sjCyyP%2BeBjqzAmZI0wG%2FJ8srx7h2HUsoKrxjJ7keZ4B6cculpXJYP4QHTszmk7afTqrQiQa4jnqeYtm5B%2BXvSKI7fDbEt9ULBUrGaxOfz1yJWVzlkwxTWkun41j8k206Sy5x8Wj8sXcCFB7R61Emt1b7oNLrtn0CCFbVCbRX28gxUj9wi9%2B2zD0zvaDmh3I8%2B1i91Kd5xqZnu8eNhL9Cb8gRt9ejg006NtrbOLZU8j3CNoXWE%2FPUVGnI8jvuOc0V%2BpC776pVlrJnSb3fGNLn8HKrlg9beGkesYZPknjvOLuZDGmfb4BrG5x0kQ%2FhDyZP1P8k2OXpCgD00yM1Sfljdr09DRC8RdsLbiS59J6YdMA2lHpFn3TR%2BeEqasXwjShtjE0lJqp4AYvcifdWXfXn9ocxvwZdueha76c5Qf0%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230205T181002Z&X-Amz-SignedHeaders=host&X-Amz-Expires=43200&X-Amz-Credential=ASIAWTA5OULQUNFPIBGM%2F20230205%2Feu-west-3%2Fs3%2Faws4_request&X-Amz-Signature=8ecb8389959226b750d7bab73f23fd8b253e38ed3db38bd195ae2060e69ae61e",
    "https://nosql-taxis.s3.eu-west-3.amazonaws.com/taxi_trips_05.csv?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEKr%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCWV1LXdlc3QtMyJHMEUCIC%2FUznqMwl9b1UEeDCzflkP%2BQOKDBxZmOX%2FIPglwOc62AiEA02cKsjDt3L6dG0Tfjgran0%2Fw1RUt6s8mNtI2KPmXme0q5AIIMhACGgw0NTMxODA4MjYzMzciDKGMEhqX47dt2ykz4irBAiuAgvQGeDTYoTfYnqLrezq83lF1j0KJEA6SP3LciRi0wsOkzx5fCDwpzwJ%2BRMMLMENMtTtgemVXxcp5yhSuz3z5g1kTRpbMQdVOZ%2BsGJ5B66k6r7NxwWjvBbKok9FUeonaZqtrlSuqoVGGRzvMnZWES0cMC8%2B8GgVv2X3h0YzHeXG8r8tVHD2z2mRxMznX%2FyIdvaTiz9ZGKwglsO6DbMVBp%2Bt4p14noN7in8ITe8%2FirSazXLGCcei2QJ0XAkam5wSoCKd7WC0M7D03LHZv%2FkIDL98MUMyY%2F89jr3XYpowgjWJMOXemm%2BJUiJEvF3YQ4OZWpiVkgZQtyJpRdF1wDIAr0wmrFSQlvlG72hQN0uSaJYw2Sy84S7pPw3LPKkhxTWS8eARvL0IwAwkx%2BVtTIb4M0cQtRWM%2FFiCWbJEM7O1%2B5sjCyyP%2BeBjqzAmZI0wG%2FJ8srx7h2HUsoKrxjJ7keZ4B6cculpXJYP4QHTszmk7afTqrQiQa4jnqeYtm5B%2BXvSKI7fDbEt9ULBUrGaxOfz1yJWVzlkwxTWkun41j8k206Sy5x8Wj8sXcCFB7R61Emt1b7oNLrtn0CCFbVCbRX28gxUj9wi9%2B2zD0zvaDmh3I8%2B1i91Kd5xqZnu8eNhL9Cb8gRt9ejg006NtrbOLZU8j3CNoXWE%2FPUVGnI8jvuOc0V%2BpC776pVlrJnSb3fGNLn8HKrlg9beGkesYZPknjvOLuZDGmfb4BrG5x0kQ%2FhDyZP1P8k2OXpCgD00yM1Sfljdr09DRC8RdsLbiS59J6YdMA2lHpFn3TR%2BeEqasXwjShtjE0lJqp4AYvcifdWXfXn9ocxvwZdueha76c5Qf0%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230205T181017Z&X-Amz-SignedHeaders=host&X-Amz-Expires=43200&X-Amz-Credential=ASIAWTA5OULQUNFPIBGM%2F20230205%2Feu-west-3%2Fs3%2Faws4_request&X-Amz-Signature=22d65bf0ee9f5393d28ca3d63af7392c5197ad3e59898394109631e50b0fcea3",
    "https://nosql-taxis.s3.eu-west-3.amazonaws.com/taxi_trips_06.csv?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEKr%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCWV1LXdlc3QtMyJHMEUCIC%2FUznqMwl9b1UEeDCzflkP%2BQOKDBxZmOX%2FIPglwOc62AiEA02cKsjDt3L6dG0Tfjgran0%2Fw1RUt6s8mNtI2KPmXme0q5AIIMhACGgw0NTMxODA4MjYzMzciDKGMEhqX47dt2ykz4irBAiuAgvQGeDTYoTfYnqLrezq83lF1j0KJEA6SP3LciRi0wsOkzx5fCDwpzwJ%2BRMMLMENMtTtgemVXxcp5yhSuz3z5g1kTRpbMQdVOZ%2BsGJ5B66k6r7NxwWjvBbKok9FUeonaZqtrlSuqoVGGRzvMnZWES0cMC8%2B8GgVv2X3h0YzHeXG8r8tVHD2z2mRxMznX%2FyIdvaTiz9ZGKwglsO6DbMVBp%2Bt4p14noN7in8ITe8%2FirSazXLGCcei2QJ0XAkam5wSoCKd7WC0M7D03LHZv%2FkIDL98MUMyY%2F89jr3XYpowgjWJMOXemm%2BJUiJEvF3YQ4OZWpiVkgZQtyJpRdF1wDIAr0wmrFSQlvlG72hQN0uSaJYw2Sy84S7pPw3LPKkhxTWS8eARvL0IwAwkx%2BVtTIb4M0cQtRWM%2FFiCWbJEM7O1%2B5sjCyyP%2BeBjqzAmZI0wG%2FJ8srx7h2HUsoKrxjJ7keZ4B6cculpXJYP4QHTszmk7afTqrQiQa4jnqeYtm5B%2BXvSKI7fDbEt9ULBUrGaxOfz1yJWVzlkwxTWkun41j8k206Sy5x8Wj8sXcCFB7R61Emt1b7oNLrtn0CCFbVCbRX28gxUj9wi9%2B2zD0zvaDmh3I8%2B1i91Kd5xqZnu8eNhL9Cb8gRt9ejg006NtrbOLZU8j3CNoXWE%2FPUVGnI8jvuOc0V%2BpC776pVlrJnSb3fGNLn8HKrlg9beGkesYZPknjvOLuZDGmfb4BrG5x0kQ%2FhDyZP1P8k2OXpCgD00yM1Sfljdr09DRC8RdsLbiS59J6YdMA2lHpFn3TR%2BeEqasXwjShtjE0lJqp4AYvcifdWXfXn9ocxvwZdueha76c5Qf0%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230205T181043Z&X-Amz-SignedHeaders=host&X-Amz-Expires=43200&X-Amz-Credential=ASIAWTA5OULQUNFPIBGM%2F20230205%2Feu-west-3%2Fs3%2Faws4_request&X-Amz-Signature=5b8e4196a4f7cb521c6b9abdb7a5e01d31ffafe4e60306f0bba8e8cb452a6fb8",
    "https://nosql-taxis.s3.eu-west-3.amazonaws.com/taxi_trips_07.csv?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEKr%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCWV1LXdlc3QtMyJHMEUCIC%2FUznqMwl9b1UEeDCzflkP%2BQOKDBxZmOX%2FIPglwOc62AiEA02cKsjDt3L6dG0Tfjgran0%2Fw1RUt6s8mNtI2KPmXme0q5AIIMhACGgw0NTMxODA4MjYzMzciDKGMEhqX47dt2ykz4irBAiuAgvQGeDTYoTfYnqLrezq83lF1j0KJEA6SP3LciRi0wsOkzx5fCDwpzwJ%2BRMMLMENMtTtgemVXxcp5yhSuz3z5g1kTRpbMQdVOZ%2BsGJ5B66k6r7NxwWjvBbKok9FUeonaZqtrlSuqoVGGRzvMnZWES0cMC8%2B8GgVv2X3h0YzHeXG8r8tVHD2z2mRxMznX%2FyIdvaTiz9ZGKwglsO6DbMVBp%2Bt4p14noN7in8ITe8%2FirSazXLGCcei2QJ0XAkam5wSoCKd7WC0M7D03LHZv%2FkIDL98MUMyY%2F89jr3XYpowgjWJMOXemm%2BJUiJEvF3YQ4OZWpiVkgZQtyJpRdF1wDIAr0wmrFSQlvlG72hQN0uSaJYw2Sy84S7pPw3LPKkhxTWS8eARvL0IwAwkx%2BVtTIb4M0cQtRWM%2FFiCWbJEM7O1%2B5sjCyyP%2BeBjqzAmZI0wG%2FJ8srx7h2HUsoKrxjJ7keZ4B6cculpXJYP4QHTszmk7afTqrQiQa4jnqeYtm5B%2BXvSKI7fDbEt9ULBUrGaxOfz1yJWVzlkwxTWkun41j8k206Sy5x8Wj8sXcCFB7R61Emt1b7oNLrtn0CCFbVCbRX28gxUj9wi9%2B2zD0zvaDmh3I8%2B1i91Kd5xqZnu8eNhL9Cb8gRt9ejg006NtrbOLZU8j3CNoXWE%2FPUVGnI8jvuOc0V%2BpC776pVlrJnSb3fGNLn8HKrlg9beGkesYZPknjvOLuZDGmfb4BrG5x0kQ%2FhDyZP1P8k2OXpCgD00yM1Sfljdr09DRC8RdsLbiS59J6YdMA2lHpFn3TR%2BeEqasXwjShtjE0lJqp4AYvcifdWXfXn9ocxvwZdueha76c5Qf0%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230205T181055Z&X-Amz-SignedHeaders=host&X-Amz-Expires=43200&X-Amz-Credential=ASIAWTA5OULQUNFPIBGM%2F20230205%2Feu-west-3%2Fs3%2Faws4_request&X-Amz-Signature=9f58390c3f019557ed81ac84bcc46aa246f3526d5a4e975238c1c0846708beec"
]


for file in s3_file_liste:
    try:
        df = extract_transform(file)
        print(df.columns)
        load(df)
    except:
        pass
