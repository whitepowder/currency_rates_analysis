import requests
import json
import pandas as pd
from sqlalchemy import create_engine
from utils.params import db_engine, db_schema

conn = create_engine(db_engine)


class ApiCall:
    def __init__(self, api=None):
        self._api = api

    def get_data(self, url):
        response = requests.get(f"{url}")
        if response.status_code == 200:
            print("Success")
            self.to_sql(response.json())
        else:
            print(
                f"Error - {response.status_code}")

    @staticmethod
    def to_sql(data):
        if 'historical' in json.dumps(data, sort_keys=True, indent=4):
            df = pd.DataFrame(data).drop(['base', 'success', 'timestamp', 'historical'], axis=1)
            df.reset_index(inplace=True)
            df.columns = ['currency', 'date', 'rates']
            df.to_sql("temp_table", con=conn, if_exists="append", index=False)
        else:
            df = pd.DataFrame(data).drop(['success', 'symbols'], axis=1)
            df.reset_index(inplace=True)
            df.columns = ['currency']
            df.to_sql("currency_data", con=conn, schema=db_schema, if_exists="append", index=False)
