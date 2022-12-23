create or replace procedure create_db()
language plpgsql
as $$
declare

BEGIN

	CREATE TABLE IF NOT EXISTS currency_data (
			currency_id serial PRIMARY KEY,
			currency text UNIQUE NOT NULL);

	CREATE TABLE IF NOT EXISTS rate (
	  		rate_id serial PRIMARY KEY,
	  		rate_date date,
	  		currency_id int REFERENCES currency_data (currency_id),
	  		rate float);

	CREATE MATERIALIZED VIEW IF NOT EXISTS MV_rates
                as
                WITH md AS (SELECT max(rate_date) as max_date from rate)
                SELECT max(rate_date)+1 as forcast_date, currency, avg(rate) as rate
                FROM rate JOIN currency_data USING (currency_id)
                WHERE rate_date BETWEEN (SELECT * FROM md)-30 and (SELECT * FROM md)
                GROUP BY currency;

    CREATE TABLE IF NOT EXISTS forcast (
  		forcast_id serial PRIMARY KEY,
  		date date,
  		currency_id int REFERENCES currency_data (currency_id),
  		rate float);

  	CREATE TABLE IF NOT EXISTS metrics_data(
	metrics_id serial PRIMARY KEY,
	date date,
	currency_id int REFERENCES currency_data (currency_id),
	r2 float,
	MAE float,
	MAPE float,
	MSE float,
	RMSE float,
	NRMSE float,
	WMAPE float);

	CREATE MATERIALIZED VIEW IF NOT EXISTS MV_metrics AS
	SELECT date, currency, r2, MAE, MAPE, MSE, RMSE, NRMSE, WMAPE from metrics_data
	JOIN currency_data USING (currency_id);

END $$;