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
session.use_schema("HARMONIZED")

# Get the DAILY_CITY_METRICS table
table = session.table("ORDERS")


df = session.sql("""
SELECT MENU_ITEM_NAME, EXTRACT(MONTH FROM ORDER_TS_DATE) AS MONTH, ROUND(SUM(ORDER_AMOUNT)) AS TOTAL_SALES
FROM HOL_DB.HARMONIZED.ORDERS
GROUP BY MENU_ITEM_NAME, EXTRACT(MONTH FROM ORDER_TS_DATE)
ORDER BY TOTAL_SALES DESC
LIMIT 10
""").collect()

print("Top 10 menu items by sales and month:")
for row in df:
    print(f"{row.MENU_ITEM_NAME} in month {row.MONTH}: {row.TOTAL_SALES}")


df_6 = session.sql("""
WITH TOTAL_ORDERS AS (
  SELECT COUNTRY, COUNT(*) AS TOTAL_ORDERS
  FROM HOL_DB.HARMONIZED.ORDERS
  GROUP BY COUNTRY
)
SELECT COUNTRY, ROUND(AVG(TOTAL_ORDERS)) AS AVG_ORDERS_PER_DAY
FROM TOTAL_ORDERS
GROUP BY COUNTRY
""").collect()

print("Average number of orders per day for each country:")
for row in df_6:
    print(f"{row.COUNTRY}: {row.AVG_ORDERS_PER_DAY}")


#Analyze customer demographics 
    
df_1 = session.sql("SELECT FRANCHISEE_FIRST_NAME, FRANCHISEE_LAST_NAME, PRIMARY_CITY, REGION, COUNTRY \
FROM HARMONIZED.ORDERS \
GROUP BY FRANCHISEE_FIRST_NAME, FRANCHISEE_LAST_NAME, PRIMARY_CITY, REGION, COUNTRY");

df_1.show()

#Calculate the total number of orders and revenue for each menu item:


df_8 = session.sql("SELECT o.MENU_ITEM_ID, o.MENU_ITEM_NAME, COUNT(*) AS ORDER_COUNT,ROUND(SUM(o.ORDER_AMOUNT)) AS TOTAL_SALES, SUM(o.QUANTITY) AS TOTAL_QUANTITY \
FROM HARMONIZED.ORDERS o \
GROUP BY o.MENU_ITEM_ID, o.MENU_ITEM_NAME \
ORDER BY ORDER_COUNT DESC")

df_8.show()


#This transformation would allow the business to see which menu items are the most popular, which could help inform decisions about menu engineering and inventory management.



# sales by Region
df_10 = session.sql("SELECT REGION, ROUND(SUM(ORDER_TOTAL)) AS TotalSales \
FROM HOL_DB.HARMONIZED.ORDERS \
GROUP BY REGION")

df_10.show()

#Sales by Date

df_11 = session.sql("SELECT DATE_TRUNC('day', ORDER_TS) AS OrderDate, ROUND(SUM(ORDER_TOTAL)) AS TotalSales \
FROM HOL_DB.HARMONIZED.ORDERS \
GROUP BY OrderDate \
ORDER BY OrderDate")

df_11.show()

#Discounted Order by Franchise ID 

df_12 = session.sql("SELECT FRANCHISE_ID, COUNT(*) AS NUM_DISCOUNTED_ORDERS \
FROM HOL_DB.HARMONIZED.ORDERS \
WHERE ORDER_DISCOUNT_AMOUNT > 0 \
GROUP BY FRANCHISE_ID \
ORDER BY NUM_DISCOUNTED_ORDERS DESC")

df_12.show()

df_13 = session.sql("SELECT PRIMARY_CITY, AVG(ORDER_AMOUNT) AS AVERAGE_ORDER_VALUE \
FROM HOL_DB.HARMONIZED.ORDERS \
GROUP BY PRIMARY_CITY \
ORDER BY AVERAGE_ORDER_VALUE DESC");

df_13.show()

