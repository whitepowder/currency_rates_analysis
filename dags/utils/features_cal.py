import pandas as pd
from sqlalchemy import create_engine
from utils.params import db_engine, db_schema, features_query

conn = create_engine(db_engine)


class Feature:
    def __init__(self, cur_list=None):
        self._cur_list = cur_list

    @staticmethod
    def feature_calc(cur_list):
        for cur in cur_list:
            df = pd.read_sql(features_query + f"""'{cur}'""", con=conn)
            df['moving_average'] = df['rate'].rolling(window=2, center=True).mean()
            df['expanding_average'] = df['rate'].expanding().mean()
            db_name = f"{cur}_features"
            df.to_sql(db_name.lower(), con=conn, schema=db_schema, if_exists="replace", index=False)
