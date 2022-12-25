import pandas as pd
from sqlalchemy import create_engine
from dataclasses import dataclass
from utils.params import db_engine, db_schema, features_query

engine = create_engine(db_engine)


@dataclass
class Feature:

    @staticmethod
    def feature_calc(currency_data):
        for currency in currency_data:
            with engine.connect() as conn:
                df = pd.read_sql(features_query + f"""'{currency}'""", con=conn)
                df['moving_average'] = df['rate'].rolling(window=2, center=True).mean()
                df['expanding_average'] = df['rate'].expanding().mean()
                db_name = f"{currency}_features"
                df.to_sql(db_name.lower(), con=conn, schema=db_schema, if_exists="replace", index=False)
                df.to_sql(db_name.lower(), con=conn, schema=db_schema, if_exists="replace", index=False)
