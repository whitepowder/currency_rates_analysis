# currency_rates_analysis
## Project implemented with use of ApacheAirflow.

This project implements day-by-day API requesting of currency rates based on the currency list with it further analysis. 
There are 3 total DAGS, one to create data base envirement (dag_create_db), to drop data base envirement (dag_drop_db) and main DAG for requeistng and analysis (dag_data_collection).
Below you can find description of the tasks in dag_data_collection DAG:
1. get_currency_list - gets the list of currencies from currencies table
2. get_currency_rates - gets the rates of currencies in the list of currencies based on the execution day of the task
3. populate_currency_table - populates currency table based on the acuqiered rates
4. features_collection - performs features analysis of the rates (rolling, expanding) for further trend plotting
5. forcasting - performs forcasting of the next-dat rate based on the monthy average
6. metrics - calculates quality metrics based on the actual and predicted rate
