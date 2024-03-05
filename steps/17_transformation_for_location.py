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
session.use_schema("RAW_POS")

# Get the DAILY_CITY_METRICS table
table = session.table("LOCATION")

df = session.sql("SELECT CITY,REGION,COUNTRY,COUNT(LOCATION_ID) AS num_locations,AVG(LENGTH(LOCATION)) AS avg_location_length \
                 FROM HOL_DB.RAW_POS.LOCATION \
                 GROUP BY CITY, REGION , COUNTRY")

df.show()


df1 = session.sql("SELECT t1.COUNTRY, COUNT(t1.LOCATION_ID) AS num_locations \
FROM HOL_DB.RAW_POS.LOCATION t1 \
INNER JOIN HOL_DB.RAW_POS.COUNTRY t2 ON t1.COUNTRY = t2.COUNTRY \
WHERE LENGTH(t1.LOCATION) > 50 AND t2.CITY_POPULATION > 10000000 \
GROUP BY t1.COUNTRY")

df1.show()

df2 = session.sql("SELECT t2.CITY, COUNT(t1.LOCATION_ID) AS num_locations \
FROM HOL_DB.RAW_POS.LOCATION t1 \
INNER JOIN HOL_DB.RAW_POS.COUNTRY t2 ON t1.COUNTRY = t2.COUNTRY \
WHERE LENGTH(t1.LOCATION) BETWEEN 20 AND 50 AND t2.CITY_POPULATION > 1000000 \
GROUP BY t2.CITY")

df2.show()


df3 = session.sql("SELECT REGION, COUNT(*) AS TOTAL_LOCATIONS \
FROM HOL_DB.RAW_POS.LOCATION \
GROUP BY REGION \
ORDER BY TOTAL_LOCATIONS DESC")

df3.show()

df4 = session.sql("SELECT CITY, COUNT(LOCATION_ID) AS NUM_LOCATIONS \
FROM HOL_DB.RAW_POS.LOCATION \
GROUP BY CITY \
ORDER BY NUM_LOCATIONS DESC \
FETCH FIRST 5 ROWS ONLY")

df4.show()

df5 = session.sql("SELECT REGION, CITY, COUNT(*) AS TOTAL_LOCATIONS \
FROM HOL_DB.RAW_POS.LOCATION \
GROUP BY REGION, CITY \
ORDER BY TOTAL_LOCATIONS DESC")

df5.show()