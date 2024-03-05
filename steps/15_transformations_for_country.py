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
table = session.table("COUNTRY")


df = session.sql("SELECT CITY, CITY_POPULATION \
FROM HOL_DB.RAW_POS.COUNTRY \
ORDER BY CITY_POPULATION DESC \
LIMIT 5 \
")

df.show()


df1 = session.sql("SELECT ISO_COUNTRY, CITY, CITY_POPULATION \
FROM ( \
    SELECT ISO_COUNTRY, CITY, CITY_POPULATION, \
        ROW_NUMBER() OVER(PARTITION BY ISO_COUNTRY ORDER BY CITY_POPULATION DESC) AS rn \
    FROM HOL_DB.RAW_POS.COUNTRY \
) t \
WHERE rn <= 5")

df1.show()

df2 = session.sql("SELECT ISO_COUNTRY, COUNT(CITY_ID) \
FROM HOL_DB.RAW_POS.COUNTRY \
WHERE CITY_POPULATION > 1000000 \
GROUP BY ISO_COUNTRY")

df2.show()

df3 = session.sql("SELECT COUNTRY, COUNT(DISTINCT CITY) AS NUM_CITIES \
FROM HOL_DB.RAW_POS.COUNTRY \
GROUP BY COUNTRY \
ORDER BY NUM_CITIES DESC")

df3.show()

df4 = session.sql("SELECT \
  COUNTRY, \
  ISO_CURRENCY \
FROM \
  HOL_DB.RAW_POS.COUNTRY")

df4.show()