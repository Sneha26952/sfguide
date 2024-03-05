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

# Get the TRUCK table
table = session.table("TRUCK")

#Truck inventory analysis: You can analyze the inventory of trucks by calculating the number of trucks by make, model, and year.

df = session.sql("SELECT MAKE,MODEL,YEAR,COUNT(*) AS truck_count \
FROM HOL_DB.RAW_POS.TRUCK \
GROUP BY MAKE, MODEL, YEAR \
ORDER BY truck_count DESC")

df.show()

#Truck usage analysis: You can analyze the usage of trucks by calculating the number of trucks by region and year.

df1 = session.sql("SELECT REGION,YEAR,COUNT(DISTINCT TRUCK_ID) AS truck_count \
FROM HOL_DB.RAW_POS.TRUCK \
GROUP BY REGION, YEAR \
ORDER BY truck_count DESC")

df1.show()

#Truck age analysis: You can analyze the age of trucks by calculating the average age of trucks by make, model, and year.

df2 = session.sql("SELECT MAKE,MODEL,YEAR,AVG(YEAR(CURRENT_DATE) - YEAR(TRUCK_OPENING_DATE)) AS avg_age \
FROM HOL_DB.RAW_POS.TRUCK \
WHERE TRUCK_OPENING_DATE IS NOT NULL \
GROUP BY MAKE, MODEL, YEAR \
ORDER BY avg_age DESC")

df2.show()

#Truck fleet analysis: You can analyze the fleet of trucks by calculating the number of trucks by franchise flag and year.

df3 = session.sql("SELECT FRANCHISE_FLAG,YEAR,COUNT(*) AS truck_count \
FROM HOL_DB.RAW_POS.TRUCK \
GROUP BY FRANCHISE_FLAG, YEAR \
ORDER BY FRANCHISE_FLAG, truck_count DESC")

df3.show()

#Truck electric vehicle (EV) analysis: You can analyze the number of electric vehicles (EVs) by calculating the number of EVs by year.

df4 = session.sql("SELECT YEAR,COUNT(*) AS ev_count \
FROM HOL_DB.RAW_POS.TRUCK \
WHERE EV_FLAG = 1 \
GROUP BY YEAR \
ORDER BY YEAR")

df4.show()