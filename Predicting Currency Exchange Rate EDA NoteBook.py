# Databricks notebook source
import pandas as pd
import numpy as np
import datetime
import matplotlib as plt
import seaborn as sns

# COMMAND ----------

#Take out all data from hive, and convert to pandas df


#DXY
dxy_table = spark.sql("SELECT * FROM Louis.dxy_table")
dxy_df = dxy_table.toPandas().sort_values(by='Date',ascending=False)
print(dxy_df.head(5))
#EUR/USD
eurusd_table = spark.sql("SELECT * FROM Louis.final_eurusd_table")
eurusd_df = eurusd_table.toPandas().sort_values(by='Date',ascending=False)
print(eurusd_df.head(5))
#US_Interest
us_interest_table = spark.sql("SELECT * FROM Louis.us_interest_table")
us_interest_df = us_interest_table.toPandas().sort_values(by='Date',ascending=False)
print(us_interest_df.head(5))
#US_Inflation
us_inflation_table = spark.sql("SELECT * FROM Louis.us_inflation_table")
us_inflation_df = us_inflation_table.toPandas().sort_values(by='Date',ascending=False)
print(us_inflation_df.head(5))
#US Real GDP
us_rgdp_table = spark.sql("SELECT * FROM Louis.us_rgdp_table")
us_rgdp_df = us_rgdp_table.toPandas().sort_values(by='Date',ascending=False)
print(us_rgdp_df.head(5))
#US_Unemployment_Rate
us_unemployment_table = spark.sql("SELECT * FROM Louis.us_unemployment_table")
us_unemployment_df = us_unemployment_table.toPandas().sort_values(by='Date',ascending=False)
print(us_unemployment_df.head(5))
#US_Government Public Debt
us_gov_debt_table = spark.sql("SELECT * FROM Louis.us_gov_debt_table")
us_gov_debt_df = us_gov_debt_table.toPandas().sort_values(by='Date',ascending=False)
print(us_gov_debt_df.head(5))

#EU
#EU Interest_Rate
eu_interest_table = spark.sql("SELECT * FROM Louis.eu_interest_table")
eu_interest_df = eu_interest_table.toPandas().sort_values(by='Date',ascending=False)
print(eu_interest_df.head(5))
#EU Inflation_Rate
eu_inflation_table = spark.sql("SELECT * FROM Louis.eu_inflation_table")
eu_inflation_df = eu_inflation_table.toPandas().sort_values(by='Date',ascending=False)
print(eu_inflation_df.head(5))
#EU Real GDP
eu_rgdp_table = spark.sql("SELECT * FROM Louis.eu_rgdp_table")
eu_rgdp_df = eu_rgdp_table.toPandas().sort_values(by='Date',ascending=False)
print(eu_rgdp_df.head(5))
#EU_Unemployment_Rate
eu_unemployment_table = spark.sql("SELECT * FROM Louis.eu_unemployment_table")
eu_unemployment_df = eu_unemployment_table.toPandas().sort_values(by='Date',ascending=False)
print(eu_unemployment_df.head(5))
#EU GDP
eu_gdp_table = spark.sql("SELECT * FROM Louis.eu_gdp_table")
eu_gdp_df = eu_gdp_table.toPandas().sort_values(by='Date',ascending=False)
print(eu_gdp_df.head(5))
#EU Debet Percentage
eu_deb_per_table = spark.sql("SELECT * FROM Louis.final_eu_deb_per_table")
eu_deb_per_df = eu_deb_per_table.toPandas().sort_values(by='Date',ascending=False)
print(eu_deb_per_df.head(5))

#Market Sentiment Data
#Commodities Index- Gold
gold_table = spark.sql("SELECT * FROM Louis.gold_table")
gold_df = gold_table.toPandas().sort_values(by='Date',ascending=False)
print(gold_df.head(5))
#Commodities Index- Crude Oil
oil_table = spark.sql("SELECT * FROM Louis.oil_table")
oil_df = oil_table.toPandas().sort_values(by='Date',ascending=False)
print(oil_df.head(5))
# Global Economic Policy Uncertainty Index
policy_uncer_idx_table = spark.sql("SELECT * FROM Louis.policy_uncer_idx_table")
policy_uncer_idx_df = policy_uncer_idx_table.toPandas().sort_values(by='Date',ascending=False)
print(policy_uncer_idx_df.head(5))
# Volatility Index
volat_idx_table = spark.sql("SELECT * FROM Louis.volat_idx_table")
volat_idx_df = volat_idx_table.toPandas().sort_values(by='Date',ascending=False)
print(volat_idx_df.head(5))
#Cusomer Confidence
consum_confid_table = spark.sql("SELECT * FROM Louis.consum_confid_table")
consum_confid_df = consum_confid_table.toPandas().sort_values(by='Date',ascending=False)
print(consum_confid_df.head(5))


# COMMAND ----------


