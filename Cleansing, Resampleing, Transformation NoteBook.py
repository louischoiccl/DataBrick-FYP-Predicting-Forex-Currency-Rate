# Databricks notebook source
import pandas as pd
import numpy as np
import datetime
import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

#Take out all data from hive, and convert to pandas df


#DXY
dxy_table1 = spark.sql("SELECT * FROM Louis.dxy_table")
dxy_df = dxy_table1.toPandas().sort_values(by='Date',ascending=False)
print(dxy_df.head(5))
#EUR/USD
eurusd_table1 = spark.sql("SELECT * FROM Louis.final_eurusd_table")
eurusd_df = eurusd_table1.toPandas().sort_values(by='Date',ascending=False)
print(eurusd_df.head(5))
#US_Interest
us_interest_table1 = spark.sql("SELECT * FROM Louis.us_interest_table")
us_interest_df = us_interest_table1.toPandas().sort_values(by='Date',ascending=False)
print(us_interest_df.head(5))
#US_Inflation
us_inflation_table1 = spark.sql("SELECT * FROM Louis.us_inflation_table")
us_inflation_df = us_inflation_table1.toPandas().sort_values(by='Date',ascending=False)
print(us_inflation_df.head(5))
#US Real GDP
us_rgdp_table1 = spark.sql("SELECT * FROM Louis.us_rgdp_table")
us_rgdp_df = us_rgdp_table1.toPandas().sort_values(by='Date',ascending=False)
print(us_rgdp_df.head(5))
#US_Unemployment_Rate
us_unemployment_table1 = spark.sql("SELECT * FROM Louis.us_unemployment_table")
us_unemployment_df = us_unemployment_table1.toPandas().sort_values(by='Date',ascending=False)
print(us_unemployment_df.head(5))
#US_Government Public Debt
us_gov_debt_table1 = spark.sql("SELECT * FROM Louis.us_gov_debt_table")
us_gov_debt_df = us_gov_debt_table1.toPandas().sort_values(by='Date',ascending=False)
print(us_gov_debt_df.head(5))

#EU
#EU Interest_Rate
eu_interest_table1 = spark.sql("SELECT * FROM Louis.eu_interest_table")
eu_interest_df = eu_interest_table1.toPandas().sort_values(by='Date',ascending=False)
print(eu_interest_df.head(5))
#EU Inflation_Rate
eu_inflation_table1 = spark.sql("SELECT * FROM Louis.eu_inflation_table")
eu_inflation_df = eu_inflation_table1.toPandas().sort_values(by='Date',ascending=False)
print(eu_inflation_df.head(5))
#EU Real GDP
eu_rgdp_table1 = spark.sql("SELECT * FROM Louis.eu_rgdp_table")
eu_rgdp_df = eu_rgdp_table1.toPandas().sort_values(by='Date',ascending=False)
print(eu_rgdp_df.head(5))
#EU_Unemployment_Rate
eu_unemployment_table1 = spark.sql("SELECT * FROM Louis.eu_unemployment_table")
eu_unemployment_df = eu_unemployment_table1.toPandas().sort_values(by='Date',ascending=False)
print(eu_unemployment_df.head(5))
#EU GDP
eu_gdp_table1 = spark.sql("SELECT * FROM Louis.eu_gdp_table1")
eu_gdp_df = eu_gdp_table1.toPandas().sort_values(by='Date',ascending=False)
print(eu_gdp_df.head(5))
#EU Debet Percentage
eu_deb_per_table1 = spark.sql("SELECT * FROM Louis.final_eu_deb_per_table")
eu_deb_per_df = eu_deb_per_table1.toPandas().sort_values(by='Date',ascending=False)
print(eu_deb_per_df.head(5))

#Market Sentiment Data
#Commodities Index- Gold
gold_table1 = spark.sql("SELECT * FROM Louis.gold_table1")
gold_df = gold_table1.toPandas().sort_values(by='Date',ascending=False)
print(gold_df.head(5))
#Commodities Index- Crude Oil
oil_table1 = spark.sql("SELECT * FROM Louis.oil_table")
oil_df = oil_table1.toPandas().sort_values(by='Date',ascending=False)
print(oil_df.head(5))
# Global Economic Policy Uncertainty Index
policy_uncer_idx_table1 = spark.sql("SELECT * FROM Louis.policy_uncer_idx_table")
policy_uncer_idx_df = policy_uncer_idx_table1.toPandas().sort_values(by='Date',ascending=False)
print(policy_uncer_idx_df.head(5))
# Volatility Index
volat_idx_table1 = spark.sql("SELECT * FROM Louis.volat_idx_table")
volat_idx_df = volat_idx_table1.toPandas().sort_values(by='Date',ascending=False)
print(volat_idx_df.head(5))
#Cusomer Confidence
consum_confid_table1 = spark.sql("SELECT * FROM Louis.consum_confid_table")
consum_confid_df = consum_confid_table1.toPandas().sort_values(by='Date',ascending=False)
print(consum_confid_df.head(5))


# COMMAND ----------

dxy_df['Date'] = pd.to_datetime(dxy_df['Date'], format='%Y-%m-%d').astype('datetime64[ns]')
gold_df['Date'] = pd.to_datetime(gold_df['Date'], format='%Y-%m-%d').astype('datetime64[ns]')
oil_df['Date'] = pd.to_datetime(oil_df['Date'], format='%Y-%m-%d').astype('datetime64[ns]')
volat_idx_df.info()


# COMMAND ----------

#Check earliest time and latest time of each dataset
data_set_list = [dxy_df,eurusd_df,
                 us_interest_df,us_inflation_df,us_rgdp_df,us_unemployment_df,us_gov_debt_df,
                 eu_interest_df,eu_inflation_df,eu_rgdp_df,eu_unemployment_df,eu_gdp_df,eu_deb_per_df,
                 gold_df,oil_df,policy_uncer_idx_df,volat_idx_df,consum_confid_df]

for df in range(len(data_set_list)):
    print(f"df{df} max:{data_set_list[df]['Date'].max()} ")
    print(f"df{df} min:{data_set_list[df]['Date'].min()} ")

# COMMAND ----------

# Creating date range
date_range = pd.date_range(start='2000-10-01', end='2022-02-01', freq='MS')

# Create DataFrame
df = pd.DataFrame(date_range, columns=['Date'])

# Exclude the last date (2022-02-01) to get the max date as 2022-01-01
date_df = df[df['Date'] != '2022-02-01'].reset_index(drop=True)

# Print DataFrame
print(date_df)
#2000-10 to 2022-01 256 months

# COMMAND ----------

#aligned to the time period from "2000-10-01" to 2022-01-01

aligned_dxy_df = dxy_df[(dxy_df['Date'] < '2022-02-01') & (dxy_df['Date'] > '2000-09-30')]
aligned_eurusd_df = eurusd_df[(eurusd_df['Date'] < '2022-02-01') & (eurusd_df['Date'] > '2000-09-30')]

aligned_us_interest_df = us_interest_df[(us_interest_df['Date'] < '2022-02-01') & (us_interest_df['Date'] > '2000-09-30')]
aligned_us_inflation_df = us_inflation_df[(us_inflation_df['Date'] < '2022-02-01') & (us_inflation_df['Date'] >= '2000-01-01')]
aligned_us_rgdp_df = us_rgdp_df[(us_rgdp_df['Date'] < '2022-02-01') & (us_rgdp_df['Date'] > '2000-09-30')]
aligned_us_unemployment_df = us_unemployment_df[(us_unemployment_df['Date'] < '2022-02-01') & (us_unemployment_df['Date'] > '2000-09-30')]
aligned_us_gov_debt_df = us_gov_debt_df[(us_gov_debt_df['Date'] < '2022-02-01') & (us_gov_debt_df['Date'] > '2000-09-30')]

aligned_eu_interest_df = eu_interest_df[(eu_interest_df['Date'] < '2022-02-01') & (eu_interest_df['Date'] > '2000-09-30')]
aligned_eu_inflation_df = eu_inflation_df[(eu_inflation_df['Date'] < '2022-02-01') & (eu_inflation_df['Date'] >= '2000-01-01')]
aligned_eu_rgdp_df = eu_rgdp_df[(eu_rgdp_df['Date'] < '2022-02-01') & (eu_rgdp_df['Date'] > '2000-09-30')]
aligned_eu_unemployment_df = eu_unemployment_df[(eu_unemployment_df['Date'] < '2022-02-01') & (eu_unemployment_df['Date'] > '2000-09-30')]
aligned_eu_gdp_df = eu_gdp_df[(eu_gdp_df['Date'] < '2022-02-01') & (eu_gdp_df['Date'] > '2000-09-30')]
aligned_eu_deb_per_df = eu_deb_per_df[(eu_deb_per_df['Date'] < '2022-02-01') & (eu_deb_per_df['Date'] > '2000-09-30')] 

aligned_gold_df = gold_df[(gold_df['Date'] < '2022-02-01') & (gold_df['Date'] > '2000-09-30')]
aligned_oil_df = oil_df[(oil_df['Date'] < '2022-02-01') & (oil_df['Date'] > '2000-09-30')]
aligned_policy_uncer_idx_df = policy_uncer_idx_df[(policy_uncer_idx_df['Date'] < '2022-02-01') & (policy_uncer_idx_df['Date'] > '2000-09-30')]
aligned_volat_idx_df = volat_idx_df[(volat_idx_df['Date'] < '2022-02-01') & (volat_idx_df['Date'] > '2000-09-30')]
aligned_consum_confid_df = consum_confid_df[(consum_confid_df['Date'] < '2022-02-01') & (consum_confid_df['Date'] > '2000-09-30')]

aligned_consum_confid_df.reset_index(drop=True)


# COMMAND ----------

aligned_policy_uncer_idx_df
#.isnull().sum()
#.info()

# COMMAND ----------

sampled_us_interest_df = aligned_us_interest_df
sampled_us_unemployment_df = aligned_us_unemployment_df
sampled_eu_unemployment_df = aligned_eu_unemployment_df
sampled_policy_uncer_idx_df = aligned_policy_uncer_idx_df
sampled_consum_confid_df = aligned_consum_confid_df

# COMMAND ----------

#Sampled EU central government Debt
eu_central_gov_debt = pd.merge(aligned_eu_gdp_df,aligned_eu_deb_per_df, on='Date',how='left').sort_values(by='Date',ascending= True)

eu_central_gov_debt = eu_central_gov_debt.fillna(method='ffill')
eu_central_gov_debt
eu_central_gov_debt.at[85,'EU_Deb%'] = eu_deb_per_df[eu_deb_per_df['Date']=='2000-01-01']['EU_Deb%']
#eu_central_gov_debt[eu_central_gov_debt['Date']=='2000-10-01']['EU_Deb%'] 
eu_central_gov_debt['EU_Central_Gov_Debt'] = eu_central_gov_debt['EU_GDP(M)']*(eu_central_gov_debt['EU_Deb%']/100)*1000000
#eu_central_gov_debt.fillna(eu_deb_per_df[eu_deb_per_df['Date']=='2000-01-01']['EU_Deb%'])
eu_central_gov_debt=eu_central_gov_debt[['Date','EU_Central_Gov_Debt']]
#eu_central_gov_debt
sampled_eu_central_gov_debt = pd.merge(date_df,eu_central_gov_debt, on='Date',how='left').sort_values(by='Date',ascending= True)
#sampled_eu_central_gov_debt
sampled_eu_central_gov_debt = sampled_eu_central_gov_debt.fillna(method='ffill')
sampled_eu_central_gov_debt

# COMMAND ----------

sampled_us_inflation_df = pd.merge(date_df,aligned_us_inflation_df, on='Date',how='left').sort_values(by='Date',ascending= True)
sampled_us_inflation_df.at[0,'US_Inflation_Rate'] = aligned_us_inflation_df[aligned_us_inflation_df['Date']=='2000-01-01']['US_Inflation_Rate']
sampled_us_inflation_df = sampled_us_inflation_df.fillna(method='ffill')
sampled_us_inflation_df

# COMMAND ----------

sampled_us_rgdp_df = pd.merge(date_df,aligned_us_rgdp_df, on='Date',how='left').sort_values(by='Date',ascending= True)
sampled_us_rgdp_df = sampled_us_rgdp_df.fillna(method='ffill')
sampled_us_rgdp_df['US_Real_GDP'] = sampled_us_rgdp_df['US_Real_GDP(B)']*1000000000
sampled_us_rgdp_df = sampled_us_rgdp_df[['Date','US_Real_GDP']]
sampled_us_rgdp_df

# COMMAND ----------

sampled_us_gov_debt_df = pd.merge(date_df,aligned_us_gov_debt_df, on='Date',how='left').sort_values(by='Date',ascending= True)

sampled_us_gov_debt_df = sampled_us_gov_debt_df.fillna(method='ffill')
sampled_us_gov_debt_df
sampled_us_gov_debt_df['US_Government_Debt'] = sampled_us_gov_debt_df['US_Government_Debt(M)']*1000000
sampled_us_gov_debt_df = sampled_us_gov_debt_df[['Date','US_Government_Debt']]
sampled_us_gov_debt_df

# COMMAND ----------

sampled_eu_inflation_df = pd.merge(date_df,aligned_eu_inflation_df, on='Date',how='left').sort_values(by='Date',ascending= True)
sampled_eu_inflation_df.at[0,'EU_Inflation_Rate'] = aligned_eu_inflation_df[aligned_eu_inflation_df['Date']=='2000-01-01']['EU_Inflation_Rate']
sampled_eu_inflation_df = sampled_eu_inflation_df.fillna(method='ffill')
sampled_eu_inflation_df

# COMMAND ----------

sampled_eu_rgdp_df = pd.merge(date_df,aligned_eu_rgdp_df, on='Date',how='left').sort_values(by='Date',ascending= True)
sampled_eu_rgdp_df = sampled_eu_rgdp_df.fillna(method='ffill')
#sampled_eu_rgdp_df
sampled_eu_rgdp_df['EU_Real_GDP'] = sampled_eu_rgdp_df['EU_Real_GDP(M)']*1000000
sampled_eu_rgdp_df = sampled_eu_rgdp_df[['Date','EU_Real_GDP']]
sampled_eu_rgdp_df

# COMMAND ----------

reset_index_aligned_dxy_df = aligned_dxy_df.set_index('Date')
reset_index_aligned_dxy_df.info()
sampled_dxy_df = reset_index_aligned_dxy_df.resample('M').agg({'US_Dollar_Index':'mean'})
sampled_dxy_df.reset_index(inplace = True)
sampled_dxy_df['Date'] = pd.to_datetime(sampled_dxy_df['Date']).dt.to_period('M').dt.to_timestamp()
sampled_dxy_df

# COMMAND ----------

reset_index_aligned_eurusd_df = aligned_eurusd_df.set_index('Date')
reset_index_aligned_eurusd_df.info()
sampled_eurusd_df = reset_index_aligned_eurusd_df.resample('M').agg({'EUR/USD':'mean'})
sampled_eurusd_df.reset_index(inplace = True)
sampled_eurusd_df['Date'] = pd.to_datetime(sampled_eurusd_df['Date']).dt.to_period('M').dt.to_timestamp()
sampled_eurusd_df


# COMMAND ----------

reset_index_eu_interest_df = aligned_eu_interest_df.set_index('Date')
reset_index_eu_interest_df.info()
sampled_eu_interest_df = reset_index_eu_interest_df.resample('M').agg({'EU_Interest_Rate':'mean'})
sampled_eu_interest_df.reset_index(inplace = True)
sampled_eu_interest_df['Date'] = pd.to_datetime(sampled_eu_interest_df['Date']).dt.to_period('M').dt.to_timestamp()
sampled_eu_interest_df


# COMMAND ----------

reset_index_gold_df = aligned_gold_df.set_index('Date')
reset_index_gold_df.info()
sampled_gold_df = reset_index_gold_df.resample('M').agg({'Commodities_Index_Gold':'mean'})
sampled_gold_df.reset_index(inplace = True)
sampled_gold_df['Date'] = pd.to_datetime(sampled_gold_df['Date']).dt.to_period('M').dt.to_timestamp()
sampled_gold_df

# COMMAND ----------

reset_index_oil_df = aligned_oil_df.set_index('Date')
reset_index_oil_df.info()
sampled_oil_df = reset_index_oil_df.resample('M').agg({'Commodities_Crude_Oil':'mean'})
sampled_oil_df.reset_index(inplace = True)
sampled_oil_df['Date'] = pd.to_datetime(sampled_oil_df['Date']).dt.to_period('M').dt.to_timestamp()
sampled_oil_df

# COMMAND ----------


reset_index_volat_idx_df = aligned_volat_idx_df.set_index('Date')
reset_index_volat_idx_df.info()
sampled_volat_idx_df = reset_index_volat_idx_df.resample('M').agg({'Volatility_Index':'mean'})
sampled_volat_idx_df.reset_index(inplace = True)
sampled_volat_idx_df['Date'] = pd.to_datetime(sampled_volat_idx_df['Date']).dt.to_period('M').dt.to_timestamp()
sampled_volat_idx_df

# COMMAND ----------

sampled_data_list = [sampled_eurusd_df,sampled_dxy_df,
                     sampled_us_interest_df,sampled_us_inflation_df,sampled_us_rgdp_df,sampled_us_unemployment_df,sampled_us_gov_debt_df,
                     sampled_eu_interest_df,sampled_eu_inflation_df,sampled_eu_rgdp_df,sampled_eu_unemployment_df,sampled_eu_central_gov_debt,
                     sampled_gold_df,sampled_oil_df,sampled_policy_uncer_idx_df,sampled_volat_idx_df,sampled_consum_confid_df]

final_sample_data_df = sampled_eurusd_df
for sampled_idx in range(1,len(sampled_data_list)):
    final_sample_data_df = final_sample_data_df.merge(sampled_data_list[sampled_idx],on = 'Date',how ='left')

final_sample_data_df.info()
#final_sample_data_sdf = spark.createDataFrame(final_sample_data_df)
#final_sample_data_sdf.write.format("Parquet").mode("append").saveAsTable("Louis.Final_Sampled_Data_Table")
