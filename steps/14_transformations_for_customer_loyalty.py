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
session.use_schema("RAW_CUSTOMER")

# Get the DAILY_CITY_METRICS table
table = session.table("CUSTOMER_LOYALTY")


df = session.sql("SELECT GENDER, \
  CASE \
    WHEN AGE BETWEEN 18 AND 24 THEN '18-24' \
    WHEN AGE BETWEEN 25 AND 34 THEN '25-34' \
    WHEN AGE BETWEEN 35 AND 44 THEN '35-44' \
    WHEN AGE BETWEEN 45 AND 54 THEN '45-54' \
    ELSE '55+' \
  END AS age_group, \
  COUNT(*) AS customer_count \
FROM ( \
  SELECT \
    GENDER, \
    FLOOR(MONTHS_BETWEEN(CURRENT_DATE, BIRTHDAY_DATE) / 12) AS AGE \
  FROM CUSTOMER_LOYALTY \
) AS ages \
GROUP BY GENDER, age_group")

df.show()

df1 = session.sql("SELECT CITY, MAX(LOYALTY_SCORE) AS max_loyalty_score, COUNT(*) AS customer_count \
FROM ( \
  SELECT CUSTOMER_ID, CITY, LOYALTY_SCORE \
  FROM CUSTOMER_LOYALTY \
) AS loyalty_scores \
GROUP BY CITY \
QUALIFY \
  ROW_NUMBER() OVER (ORDER BY MAX(LOYALTY_SCORE) DESC, COUNT(*) DESC) <= 5")

df1.show()

df2 = session.sql("SELECT CITY, CUSTOMER_ID, LOYALTY_SCORE \
FROM ( \
  SELECT CITY, CUSTOMER_ID, LOYALTY_SCORE, \
         ROW_NUMBER() OVER (PARTITION BY CITY ORDER BY LOYALTY_SCORE DESC) AS rank \
  FROM CUSTOMER_LOYALTY \
) AS loyalty_scores \
WHERE rank <= 3")

df2.show()

df3 = session.sql("SELECT MARITAL_STATUS, COUNT(*) AS customer_count \
FROM CUSTOMER_LOYALTY \
GROUP BY MARITAL_STATUS")

df3.show()

df4 = session.sql("SELECT PREFERRED_LANGUAGE, COUNT(*) AS customer_count \
FROM CUSTOMER_LOYALTY \
GROUP BY PREFERRED_LANGUAGE")

df4.show()

#Business use case: This transformation helps the business understand the distribution of customers by preferred language. This information can be used to develop marketing strategies that target specific language groups.
