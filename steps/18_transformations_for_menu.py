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
table = session.table("MENU")

df = session.sql("SELECT MENU_TYPE, COUNT(MENU_ITEM_ID) AS num_menu_items, AVG(COST_OF_GOODS_USD) AS avg_cost_of_goods \
FROM HOL_DB.RAW_POS.MENU \
GROUP BY MENU_TYPE")

df.show()

df1 = session.sql("SELECT MENU_TYPE, COUNT(MENU_ITEM_ID) AS num_menu_items, AVG(SALE_PRICE_USD) AS avg_sale_price \
FROM HOL_DB.RAW_POS.MENU \
WHERE COST_OF_GOODS_USD BETWEEN 1 AND 20 \
GROUP BY MENU_TYPE")

df1.show()

df2 = session.sql("SELECT MENU_TYPE,MENU_ITEM_NAME,TRUCK_BRAND_NAME,ITEM_CATEGORY,ITEM_SUBCATEGORY,COST_OF_GOODS_USD,SALE_PRICE_USD  \
FROM HOL_DB.RAW_POS.MENU \
WHERE MENU_TYPE = 'Sandwiches'")

df2.show()

df3 = session.sql("SELECT ITEM_CATEGORY, AVG(SALE_PRICE_USD - COST_OF_GOODS_USD) AS AVG_PROFIT_MARGIN \
FROM HOL_DB.RAW_POS.MENU \
GROUP BY ITEM_CATEGORY")

df3.show()

df4 = session.sql("SELECT TRUCK_BRAND_NAME, COUNT(DISTINCT MENU_ITEM_ID) AS NUM_MENU_ITEMS \
FROM HOL_DB.RAW_POS.MENU \
GROUP BY TRUCK_BRAND_NAME")

df4.show()

df5 = session.sql("SELECT TRUCK_BRAND_NAME, COUNT(DISTINCT MENU_ITEM_ID) AS NUM_UNIQUE_MENU_ITEMS \
                  FROM HOL_DB.RAW_POS.MENU \
                 GROUP BY TRUCK_BRAND_NAME")

df5.show()