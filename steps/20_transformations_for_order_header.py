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

# Get the ORDER_DETAIL table
table = session.table("ORDER_HEADER")

df = session.sql("SELECT d.MENU_ITEM_ID, AVG(d.QUANTITY) AS avg_quantity \
FROM HOL_DB.RAW_POS.ORDER_DETAIL d \
JOIN HOL_DB.RAW_POS.ORDER_HEADER o ON d.ORDER_ID = o.ORDER_ID \
GROUP BY d.MENU_ITEM_ID \
ORDER BY avg_quantity DESC")

df.show()

df1 = session.sql("SELECT TO_CHAR(o.ORDER_TS, 'Day') AS day_of_week, EXTRACT(HOUR FROM o.ORDER_TS) AS hour_of_day,SUM(d.PRICE) AS total_revenue \
FROM HOL_DB.RAW_POS.ORDER_HEADER o \
JOIN HOL_DB.RAW_POS.ORDER_DETAIL d ON o.ORDER_ID = d.ORDER_ID \
GROUP BY TO_CHAR(o.ORDER_TS, 'Day'),EXTRACT(HOUR FROM o.ORDER_TS) \
ORDER BY total_revenue DESC")

df1.show()

#total revenue for each truck, location and shift 

df2 = session.sql("SELECT LOCATION_ID, TRUCK_ID, SHIFT_ID, SUM(ORDER_AMOUNT) AS total_revenue \
FROM HOL_DB.RAW_POS.ORDER_HEADER \
GROUP BY LOCATION_ID, TRUCK_ID, SHIFT_ID")

df2.show()

df3 = session.sql("SELECT EXTRACT(HOUR FROM ORDER_TS) AS hour,SUM(ORDER_AMOUNT) AS total_amount \
FROM HOL_DB.RAW_POS.ORDER_HEADER \
GROUP BY hour \
ORDER BY hour")

df3.show()

df4 = session.sql("SELECT ORDER_TS,ORDER_AMOUNT,LAG(ORDER_AMOUNT) OVER (ORDER BY ORDER_TS) AS prev_order_amount,(ORDER_AMOUNT - LAG(ORDER_AMOUNT) OVER (ORDER BY ORDER_TS)) / LAG(ORDER_AMOUNT) OVER (ORDER BY ORDER_TS) * 100 AS pct_change \
FROM HOL_DB.RAW_POS.ORDER_HEADER \
ORDER BY ORDER_TS")

df4.show()

df5 = session.sql("SELECT \
    CASE \
        WHEN ORDER_AMOUNT < 100 THEN 'low' \
        WHEN ORDER_AMOUNT BETWEEN 100 AND 500 THEN 'medium' \
        ELSE 'high'\
    END AS order_segment, \
    COUNT(*) AS order_count,\
    SUM(ORDER_AMOUNT) AS total_amount \
FROM HOL_DB.RAW_POS.ORDER_HEADER \
GROUP BY order_segment \
ORDER BY order_count DESC")

df5.show()
