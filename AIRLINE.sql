--This is a new dataset and i am going to first create a table for it 
--before importing the data rather than just importing the flat file 
--as a new table directly.

--CREATE DATABASE AIRLINE

CREATE DATABASE AIRLINE

-- THEN, WE CREATE THE TABLE CONTAINING ALL COLUNMS

CREATE TABLE Delay_causes
(
	year INT,
	month INT,
	carrier VARCHAR(5),
	carrier_name VARCHAR(45),
	airport VARCHAR(10),
	airport_name VARCHAR(75),
	arr_flights FLOAT,
	arr_del15 FLOAT,
	carrier_ct FLOAT,
	weather_ct FLOAT,
	nas_ct FLOAT,
	security_ct FLOAT,
	late_aircraft_ct FLOAT,
	arr_cancelled FLOAT,
	arr_diverted FLOAT,
	arr_delay FLOAT,
	carrier_delay FLOAT,
	weather_delay FLOAT,
	nas_delay FLOAT,
	security_delay FLOAT,
	late_aircraft_delay FLOAT,
)

-- LET US SEE THE TABLE.

select * 
from Delay_causes

-- NEXT, IS IPORTING DATA INTO THE TABLE AND CONFIRMAMING THE ROWS

select * 
from Delay_causes

select count(year) 
from Delay_causes

-- NEXT IS DATA CLEANING 
-- Check for null values

select *
from Delay_causes
where year is null or weather_ct is null or month is null or carrier is null
	or carrier_name is null or airport is null or airport_name is null 
	or arr_flights is null or	arr_del15 is null or	carrier_ct is null 
	or weather_ct is null or	nas_ct is null or	security_ct is null 
	or	late_aircraft_ct is null or arr_cancelled is null or	arr_diverted is null 
	or	arr_delay is null or carrier_delay is null or	weather_delay is null 
	or nas_delay is null or security_delay is null or	late_aircraft_delay	is null

-- Fill down the null values using miltiple CTEs

create view final_table as
with number as (
	select *,
	row_number() over (order by year) as num
	from Delay_causes
	),
	 grouped_data as (
	select year, month, carrier, carrier_name, airport, airport_name,arr_flights, arr_del15,
			carrier_ct, weather_ct,nas_ct, security_ct, late_aircraft_ct,
			arr_cancelled,arr_diverted, arr_delay, carrier_delay, weather_delay, nas_delay,
			security_delay, late_aircraft_delay, num,
	count(arr_flights) over (order by num) as cl_arr_flights,
	count(arr_del15) over (order by num) as cl_arr_del15,
	count(carrier_ct) over (order by num) as cl_carrier_ct,
	count(weather_ct) over (order by num) as cl_weather_ct,
	count(nas_ct) over (order by num) as cl_nas_ct,
	count(security_ct) over (order by num) as cl_security_ct,
	count(late_aircraft_ct) over (order by num) as cl_late_aircraft_ct,
	count(arr_cancelled) over (order by num) as cl_arr_cancelled,
	count(arr_diverted) over (order by num) as cl_arr_diverted,
	count(arr_delay) over (order by num) as cl_arr_delay,
	count(carrier_delay) over (order by num) as cl_carrier_delay, 
	count(weather_delay) over (order by num) as cl_weather_delay,
	count(nas_delay) over (order by num) as cl_nas_delay,
	count(security_delay) over (order by num) as cl_security_delay,
	count(late_aircraft_delay) over (order by num) as cl_late_aircraft_delay
	from number
	),
	Cleaned_data as (
	select year, month, carrier, carrier_name, airport, airport_name,
		First_value(arr_flights) over (partition by cl_arr_flights order by num) as cle_arr_flights,
		First_value(arr_del15) over (partition by cl_arr_del15 order by num) as cle_arr_del15,
		First_value(carrier_ct) over (partition by cl_carrier_ct order by num) as cle_carrier_ct,
		First_value(weather_ct) over (partition by cl_weather_ct order by num) as cle_weather_ct,
		First_value(nas_ct) over (partition by cl_nas_ct order by num) as cle_nas_ct,
		First_value(security_ct) over (partition by cl_security_ct order by num) as cle_security_ct,
		First_value(late_aircraft_ct) over (partition by cl_late_aircraft_ct order by num) as cle_late_aircraft_ct,
		First_value(arr_cancelled) over (partition by cl_arr_cancelled order by num) as cle_arr_cancelled,
		First_value(arr_diverted) over (partition by cl_arr_diverted order by num) as cle_arr_diverted,
		First_value(arr_delay) over (partition by cl_arr_delay order by num) as cle_arr_delay,
		First_value(carrier_delay) over (partition by cl_carrier_delay order by num) as cle_carrier_delay, 
		First_value(weather_delay) over (partition by cl_weather_delay order by num) as cle_weather_delay,
		First_value(nas_delay) over (partition by cl_nas_delay order by num) as cle_nas_delay,
		First_value(security_delay) over (partition by cl_security_delay order by num) as cle_security_delay,
		First_value(late_aircraft_delay) over (partition by cl_late_aircraft_delay order by num) as cle_late_aircraft_delay
	from grouped_data
	)
	select *
	from Cleaned_data
	
-- VIEWING THE FINAL TABLE AFTER CLEANING
	
	select *
	from final_table
	
-- LET NOW EXPLORE THE DATA

-- 1. How many airports are recorded?
select count(airport) as Total_airport
from final_table

-- 2. Show top 2 airport with the highest securty delays
select Top 2 airport, sum(cle_security_delay) as sec_delays
from final_table
group by airport
order by sec_delays desc

-- 3. Which month and year experienced the highest weather delay
select year, month, sum(cle_weather_delay) as weather_delay
from final_table
group by year,month
order by weather_delay desc

-- 4. show the total carrier delay in 2024 and on 1st month
select year, month, sum(cle_carrier_delay) as Total_delay
from final_table
where year=2025 and month = 1
group by year, month

-- 5. What year experienced less than 4922000 late aircraft delays
select year, sum(cle_late_aircraft_delay) as late_aircraft
from final_table
group by year
having sum(cle_late_aircraft_delay) < 4922000
order by year