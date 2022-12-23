create or replace procedure populate_currency_table()
language plpgsql
as $$
declare

BEGIN

    ALTER TABLE temp_table
    ALTER COLUMN date TYPE date USING date::date;

    INSERT INTO rate (rate_date, currency_id, rate)
    SELECT date, currency_id, rates FROM temp_table
    JOIN currency_data USING (currency);

    DROP TABLE IF EXISTS temp_table;

END $$;