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
table = session.table("ORDER_DETAIL")

df = session.sql("SELECT TO_CHAR(ORDER_TS, 'Day') AS day_of_week, SUM(PRICE) AS total_revenue \
FROM HOL_DB.RAW_POS.ORDER_DETAIL \
GROUP BY TO_CHAR(ORDER_TS, 'Day') \
ORDER BY total_revenue DESC") 

df.show()

#Business use case: This query can be used to analyze the total revenue for each day of the week, which can help identify which days of the week have the highest revenue.

df1 = session.sql("SELECT MENU_ITEM_ID, SUM(PRICE) AS total_revenue \
FROM HOL_DB.RAW_POS.ORDER_DETAIL \
GROUP BY MENU_ITEM_ID \
ORDER BY total_revenue DESC")

df1.show()

#Business use case: This query can be used to analyze the total revenue for each order, which can help identify which orders generate the most revenue.


df2 = session.sql("SELECT TO_CHAR(ORDER_TS, 'Day') AS day_of_week, COUNT(*) AS num_orders \
FROM HOL_DB.RAW_POS.ORDER_DETAIL \
GROUP BY TO_CHAR(ORDER_TS, 'Day') \
ORDER BY num_orders DESC")

df2.show()


df3 = session.sql("SELECT ORDER_ID, SUM(QUANTITY * PRICE) AS TOTAL_PRICE \
FROM HOL_DB.RAW_POS.ORDER_DETAIL \
GROUP BY ORDER_ID \
ORDER BY TOTAL_PRICE DESC \
LIMIT 10")

df3.show()

df4 = session.sql("WITH ranked_menu_items AS ( \
  SELECT MENU_ITEM_ID, SUM(QUANTITY) AS TOTAL_QUANTITY, \
         ROW_NUMBER() OVER (ORDER BY SUM(QUANTITY) DESC) AS rn \
  FROM HOL_DB.RAW_POS.ORDER_DETAIL \
  GROUP BY MENU_ITEM_ID \
) \
SELECT MENU_ITEM_ID, TOTAL_QUANTITY \
FROM ranked_menu_items \
WHERE rn <= 5")

df4.show()

df5 = session.sql("SELECT ORDER_ID, SUM(PRICE) AS TOTAL_REVENUE, SUM(PRICE - (QUANTITY * UNIT_PRICE/100)) AS TOTAL_PROFIT \
FROM HOL_DB.RAW_POS.ORDER_DETAIL \
GROUP BY ORDER_ID")

df5.show()

