from snowflake.snowpark import Session

# Create a Snowpark session and establish a connection to Snowflake
connection_params = {
    "account": "lv39945.central-india.azure",
    "user": "AnishaG",
    "password": "Anishag26@123",
    "timeout" : 60,
}

session = Session.builder.configs(connection_params).create()

# Switch to the HOL_DB database
session.use_database("HOL_DB")

# Switch to the ANALYTICS schema
session.use_schema("ANALYTICS")

# Get the DAILY_CITY_METRICS table
table = session.table("DAILY_CITY_METRICS")

# Use case 1: Calculate the total sales for each country
total_sales_by_country = table.group_by("COUNTRY_DESC").agg({"DAILY_SALES": "sum"}).collect()
print("Total sales by country:")
for row in total_sales_by_country:
    print(row)


# Use case 2: Calculate the average temperature for each country
avg_temp_by_country = table.group_by("COUNTRY_DESC").agg({"AVG_TEMPERATURE_FAHRENHEIT": "avg"}).collect()
print("Average temperature by country:")
for row in avg_temp_by_country:
    print(row)



#Find the average temperature for each country, grouped by month
df = session.sql("SELECT COUNTRY_DESC, EXTRACT(MONTH FROM DATE) AS MONTH, AVG(AVG_TEMPERATURE_FAHRENHEIT) AS AVG_TEMPERATURE_FAHRENHEIT \
FROM HOL_DB.ANALYTICS.DAILY_CITY_METRICS \
GROUP BY COUNTRY_DESC, EXTRACT(MONTH FROM DATE) \
ORDER BY COUNTRY_DESC, MONTH")

df.show()

#Find the maximum wind speed for each day, grouped by country
df_1 = session.sql("SELECT COUNTRY_DESC, TO_CHAR(DATE, 'YYYY-MM-DD') AS DAY, MAX(MAX_WIND_SPEED_100M_MPH) AS MAX_MAX_WIND_SPEED_100M_MPH \
FROM HOL_DB.ANALYTICS.DAILY_CITY_METRICS \
GROUP BY COUNTRY_DESC, TO_CHAR(DATE, 'YYYY-MM-DD') \
ORDER BY COUNTRY_DESC, DAY")

df_1.show()

#Find the number of days with temperature above 90 degrees Fahrenheit, grouped by country
df_2 = session.sql("SELECT COUNTRY_DESC, COUNT(*) AS NUMBER_OF_HOT_DAYS \
FROM HOL_DB.ANALYTICS.DAILY_CITY_METRICS \
WHERE AVG_TEMPERATURE_FAHRENHEIT > 90 \
GROUP BY COUNTRY_DESC")

df_2.show()

df_3 = session.sql("""
SELECT COUNTRY_DESC, EXTRACT(MONTH FROM DATE) AS MONTH, SUM(DAILY_SALES) AS TOTAL_SALES
FROM HOL_DB.ANALYTICS.DAILY_CITY_METRICS
GROUP BY COUNTRY_DESC, EXTRACT(MONTH FROM DATE)
ORDER BY COUNTRY_DESC, MONTH
""").collect()

print("Total sales by country and month:")
for row in df_3:
    print(f"{row.COUNTRY_DESC} in month {row.MONTH}: {row.TOTAL_SALES}")


    



