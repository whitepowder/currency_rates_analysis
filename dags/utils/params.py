db_engine = 'postgresql+psycopg2://postgres:airflow@host.docker.internal:5432/postgres'
db_schema = 'public'
currency_link = 'https://api.apilayer.com/fixer/symbols?apikey='
features_query = """SELECT rate_id, rate_date, rate, rate.currency_id as currency_id FROM rate
JOIN currency_data USING (currency_id) WHERE currency_data.currency LIKE """
