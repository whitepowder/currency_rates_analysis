create or replace procedure forcasting()
language plpgsql
as $$
declare

BEGIN

    REFRESH MATERIALIZED VIEW MV_rates;

    insert into forcast (date, currency_id, rate)
	select forcast_date, currency_id, rate from MV_rates
	JOIN currency_data USING (currency);

END $$;
