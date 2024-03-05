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
table = session.table("FRANCHISE")


df = session.sql("SELECT COUNTRY, COUNT(FRANCHISE_ID) AS franchise_count \
FROM HOL_DB.RAW_POS.FRANCHISE \
GROUP BY COUNTRY")

df.show()

df1 = session.sql("SELECT FRANCHISE_ID, CONCAT(FIRST_NAME, ' ', LAST_NAME) AS FULL_NAME, CITY, COUNTRY, E_MAIL, PHONE_NUMBER \
FROM HOL_DB.RAW_POS.FRANCHISE")

df1.show()

df2  = session.sql("SELECT COUNTRY, COUNT(FRANCHISE_ID) AS num_franchises \
FROM HOL_DB.RAW_POS.FRANCHISE \
WHERE E_MAIL LIKE '%@gmail.com' \
GROUP BY COUNTRY") 

df2.show()

df3 = session.sql("SELECT t1.COUNTRY, AVG(LENGTH(t1.E_MAIL)) AS avg_email_length \
FROM HOL_DB.RAW_POS.FRANCHISE t1 \
WHERE E_MAIL IS NOT NULL AND PHONE_NUMBER IS NOT NULL \
GROUP BY t1.COUNTRY")

df3.show()

df4 = session.sql("SELECT t1.COUNTRY, t1.CITY, COUNT(t1.FRANCHISE_ID) AS num_franchises \
FROM HOL_DB.RAW_POS.FRANCHISE t1 \
INNER JOIN HOL_DB.RAW_POS.COUNTRY t2 ON t1.COUNTRY = t2.COUNTRY \
WHERE E_MAIL IS NOT NULL AND PHONE_NUMBER IS NOT NULL AND t2.CITY_POPULATION > 1000000 \
GROUP BY t1.COUNTRY, t1.CITY")

df4.show()

#This query can be used to analyze the average population of the cities where franchises that have both an e-mail address and a phone number are located. This information can be used to identify countries with a high or low average population of cities where franchises with those criteria are located.