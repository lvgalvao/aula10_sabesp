import requests
from pandas import DataFrame
from typing import Dict
import pandas as pd
from time import sleep
from pydantic import validate_call

from meu_log import log_decorator

URL = "https://api.coinbase.com/v2/prices/spot?currency=USD#"

@log_decorator
@validate_call
def extract(URL: int) -> Dict:
    response = requests.get(url=URL)
    data_dict = response.json()
    return data_dict

@log_decorator
def transform(data_dict: dict) -> DataFrame:
    data = data_dict
    dataframe = pd.DataFrame([data["data"]])
    dataframe["timestampt"] = pd.Timestamp.now()
    return dataframe

@log_decorator
def load(dataframe: DataFrame):
    dataframe.to_csv("coleta_bitcoin.csv", 
                     index=False, 
                     mode='a',
                     header=False
                     )

@log_decorator
def pipeline(URL):
    data_dict_get = extract(URL)
    data_dataframe = transform(data_dict_get)
    load(data_dataframe)

if __name__ == "__main__":
    while True:
        pipeline(URL)
        sleep(5)