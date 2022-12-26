import requests
import json
import pandas as pd
from sqlalchemy import create_engine
from dataclasses import dataclass
from utils.params import db_engine, db_schema, url

engine = create_engine(db_engine)


@dataclass
class ApiCall:
    apikey: str

    def get_data(self, endpoint):
        headers = {
            "apikey": self.apikey
        }
        response = requests.get(url + endpoint, headers=headers)
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
            with engine.connect() as conn:
                df.to_sql("temp_table", con=conn, if_exists="append", index=False)
        else:
            df = pd.DataFrame(data).drop(['success', 'symbols'], axis=1)
            df.reset_index(inplace=True)
            df.columns = ['currency']
            with engine.connect() as conn:
                df.to_sql("currency_data", con=conn, schema=db_schema, if_exists="append", index=False)


if __name__ == "__main__":
    pass
