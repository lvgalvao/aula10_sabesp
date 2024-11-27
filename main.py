import requests
from pandas import DataFrame
from typing import Dict
import pandas as pd
from time import sleep

URL = "https://api.coinbase.com/v2/prices/spot?currency=USD#"

def extract(URL: str) -> Dict:
    print("Começou o extract")
    response = requests.get(url=URL)
    data_dict = response.json()
    return data_dict

def transform(data_dict: dict) -> DataFrame:
    print("Começou a transformação")
    data = data_dict
    dataframe = pd.DataFrame([data["data"]])
    dataframe["timestampt"] = pd.Timestamp.now()
    return dataframe

def load(dataframe: DataFrame):
    print("Começou o load")
    dataframe.to_csv("coleta_bitcoin.csv", 
                     index=False, 
                     mode='a',
                     header=False
                     )
    
def pipeline(URL):
    print("Começou a pipeline")
    data_dict_get = extract(URL)
    data_dataframe = transform(data_dict_get)
    load(data_dataframe)
    print("Acabou a pipeline")

if __name__ == "__main__":
    while True:
        pipeline(URL)
        sleep(5)