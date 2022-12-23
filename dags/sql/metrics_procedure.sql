create or replace procedure metrics()
language plpgsql
as $$
declare

BEGIN

	DROP TABLE IF EXISTS a_vs_p;
	CREATE TEMP TABLE IF NOT EXISTS a_vs_p(
	date date,
	currency_id int,
	actual float,
	predicted float,
	a_avg float);

	INSERT INTO a_vs_p
	SELECT rate_date, currency_id, r.rate as actual, f.rate as predicted, avg(r.rate) over (partition by currency_id) as a_avg
	FROM rate r JOIN forcast f USING (currency_id)
	WHERE rate_date = f.date;

	DROP TABLE IF EXISTS r2;
	CREATE TEMP TABLE r2 (
	date date,
	currency_id int,
	r2 float);


	DO
	$do$
	BEGIN
	   IF (SELECT SUM(actual - a_avg) FROM a_vs_p)!=0 THEN
	      	INSERT INTO r2
			SELECT max(date), currency_id, 1-(SUM(POWER(actual - predicted,2)))/(SUM(POWER(actual - a_avg,2)))
			FROM a_vs_p
			GROUP BY currency_id;
	   ELSE
	        INSERT INTO r2
			SELECT max(date), currency_id, NULL
			FROM a_vs_p
			GROUP BY currency_id;
	   END IF;
	END
	$do$;

	DROP TABLE IF EXISTS MAE;
	CREATE TEMP TABLE MAE (
	date date,
	currency_id int,
	MAE float);

	INSERT INTO MAE
	SELECT max(date), currency_id, SUM(predicted - actual)/COUNT(actual) as MAE
	FROM a_vs_p
	GROUP BY currency_id;


	DROP TABLE IF EXISTS MAPE;
	CREATE TEMP TABLE MAPE (
	date date,
	currency_id int,
	MAPE float);

	INSERT INTO MAPE
	SELECT max(date), currency_id, (100/COUNT(actual))*SUM((actual-predicted)/actual) as MAPE
	FROM a_vs_p
	GROUP BY currency_id;



	DROP TABLE IF EXISTS MSE;
	CREATE TEMP TABLE MSE (
	date date,
	currency_id int,
	MSE float);

	INSERT INTO MSE
	SELECT max(date), currency_id, (SUM(POWER(actual-predicted,2)))/COUNT(actual) as MSE
	FROM a_vs_p
	GROUP BY currency_id;



	DROP TABLE IF EXISTS RMSE;
	CREATE TEMP TABLE RMSE (
	date date,
	currency_id int,
	RMSE float);

	INSERT INTO RMSE
	SELECT max(date), currency_id, SQRT((SUM(POWER(actual-predicted,2)))/COUNT(actual)) as RMSE
	FROM a_vs_p
	GROUP BY currency_id;



	DROP TABLE IF EXISTS NRMSE;
	CREATE TEMP TABLE NRMSE (
	date date,
	currency_id int,
	NRMSE float);


	DO
	$do$
	BEGIN
	   IF (SELECT SUM(n) FROM (SELECT MAX(actual) - MIN(actual) as n FROM a_vs_p GROUP BY currency_id)t)!=0 THEN
	      	INSERT INTO NRMSE
			SELECT max(date), currency_id, (SQRT((SUM(POWER(actual-predicted,2)))/COUNT(actual)))/(MAX(actual)-MIN(actual)) as NRMSE
			FROM a_vs_p
			GROUP BY currency_id;
	   ELSE
	        INSERT INTO NRMSE
			SELECT max(date), currency_id, NULL
			FROM a_vs_p
			GROUP BY currency_id;
	   END IF;
	END
	$do$;

	DROP TABLE IF EXISTS WMAPE;
	CREATE TEMP TABLE WMAPE (
	date date,
	currency_id int,
	WMAPE float);

	INSERT INTO WMAPE
	SELECT max(date), currency_id, (SUM(actual-predicted))/(SUM(actual)) as WMAPE
	FROM a_vs_p
	GROUP BY currency_id;

	INSERT INTO metrics_data (date, currency_id, r2, MAE, MAPE, MSE, RMSE, NRMSE, WMAPE)
	SELECT r2.date, r2.currency_id, r2, MAE, MAPE, MSE, RMSE, NRMSE, WMAPE from r2
	JOIN MAE ON r2.date = MAE.date AND r2.currency_id=MAE.currency_id
	JOIN MAPE ON r2.date = MAPE.date AND r2.currency_id=MAPE.currency_id
	JOIN MSE ON r2.date = MSE.date AND r2.currency_id=MSE.currency_id
	JOIN RMSE ON r2.date = RMSE.date AND r2.currency_id=RMSE.currency_id
	JOIN NRMSE ON r2.date = NRMSE.date AND r2.currency_id=NRMSE.currency_id
	JOIN WMAPE ON r2.date = WMAPE.date AND r2.currency_id=WMAPE.currency_id;

	REFRESH MATERIALIZED VIEW MV_metrics;

END $$;