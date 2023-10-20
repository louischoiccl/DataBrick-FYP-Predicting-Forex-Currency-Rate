# Databricks notebook source
# MAGIC %pip install yfinance

# COMMAND ----------

import pandas as pd
import requests
import json
import datetime
import numpy as np
import yfinance as yf

# COMMAND ----------

fred_api_key = "96955cdc0625c9eda2d4715ddda17cf7"
alpha_api_key = "7LAK553ODBAXK36Q"

# COMMAND ----------

# US Dollar Index
tickerSymbol = "DX-Y.NYB"

# Get data on this ticker
tickerData = yf.Ticker(tickerSymbol)

# Get the historical prices for this ticker
dxy_df = (
    tickerData.history(period="1d", start="1999-1-1")
    .reset_index()
    .sort_values(by="Date", ascending=False)[["Date", "Close"]]
    .rename(columns={"Date": "Date", "Close": "US_Dollar_Index"})
)
dxy_df["Date"] = pd.to_datetime(dxy_df["Date"], format="%Y-%m-%d").dt.strftime(
    "%Y-%m-%d"
)
dxy_df["US_Dollar_Index"] = dxy_df["US_Dollar_Index"].astype(float)

# See your data
#dxy_df
dxy_sdf = spark.createDataFrame(dxy_df)
dxy_sdf.write.format("Parquet").mode("append").saveAsTable("Louis.dxy_table")

# COMMAND ----------

#EUR/USD
eurusd_series_id = "DEXUSEU"

fred_eurusd_url = f"https://api.stlouisfed.org/fred/series/observations?series_id={eurusd_series_id}&api_key={fred_api_key}&file_type=json"
eurusd_exchange_rate_r = requests.get(fred_eurusd_url)
eurusd_exchange_rate_json = eurusd_exchange_rate_r.json()
eurusd_exchange_rate = eurusd_exchange_rate_json['observations']
#eurusd_exchange_rate
eurusd_df = pd.DataFrame(eurusd_exchange_rate).sort_values(by=['date'],ascending=False).reset_index().iloc[:, [3, 4]].rename(columns={"date": "Date", "value":"EUR/USD"})
eurusd_df = eurusd_df.replace('.',np.NaN)
eurusd_df['Date'] = pd.to_datetime(eurusd_df['Date'], format='%Y-%m-%d')
eurusd_df['EUR/USD'] = eurusd_df['EUR/USD'].astype(float)
#eurusd_df


# COMMAND ----------

#Real Time EUR/USD from Alpha Vantage
alpha_url = f"https://www.alphavantage.co/query?function=FX_DAILY&from_symbol=EUR&to_symbol=USD&outputsize=full&apikey={alpha_api_key}"
alpha_eurusd_exchange_rate_r = requests.get(alpha_url)
alpha_eurusd_exchange_rate_json = alpha_eurusd_exchange_rate_r.json()
alpha_eurusd_exchange_rate = alpha_eurusd_exchange_rate_json['Time Series FX (Daily)']
alpha_eurusd_df = pd.DataFrame(alpha_eurusd_exchange_rate).T.reset_index().iloc[:,[0,4]].rename(columns={"index": "Date", "4. close":"EUR/USD"})
alpha_eurusd_df['Date'] = pd.to_datetime(alpha_eurusd_df['Date'], format='%Y-%m-%d')
alpha_eurusd_df['EUR/USD'] = alpha_eurusd_df['EUR/USD'].astype(float)
alpha_eurusd_df = alpha_eurusd_df[alpha_eurusd_df['Date'] > eurusd_df.iloc[0]['Date']]

final_eurusd_df = pd.concat([alpha_eurusd_df,eurusd_df])
#final_eurusd_df

final_eurusd_sdf = spark.createDataFrame(final_eurusd_df)
final_eurusd_sdf.write.format("Parquet").mode("append").saveAsTable("Louis.final_eurusd_table")

# COMMAND ----------

#US Interest_Rate
us_interest_id = "FEDFUNDS"

us_interest_url = f"https://api.stlouisfed.org/fred/series/observations?series_id={us_interest_id}&api_key={fred_api_key}&file_type=json"
us_interest_r = requests.get(us_interest_url)
us_interest_json = us_interest_r.json()
us_interest = us_interest_json['observations']
us_interest_df = pd.DataFrame(us_interest).sort_values(by=['date'],ascending=False).reset_index().iloc[:, [3, 4]].rename(columns={"date": "Date", "value":"US_Interest_Rate"})
us_interest_df['Date'] = pd.to_datetime(us_interest_df['Date'], format='%Y-%m-%d')
us_interest_df['US_Interest_Rate'] = us_interest_df['US_Interest_Rate'].astype(float)
#us_interest_df.head(10)

us_interest_sdf = spark.createDataFrame(us_interest_df)
us_interest_sdf.write.format("Parquet").mode("append").saveAsTable("Louis.us_interest_table")

# COMMAND ----------

#US Inflation Rate
us_inflation_id = "FPCPITOTLZGUSA"

us_inflation_url = f"https://api.stlouisfed.org/fred/series/observations?series_id={us_inflation_id}&api_key={fred_api_key}&file_type=json"
us_inflation_r = requests.get(us_inflation_url)
us_inflation_json = us_inflation_r.json()
us_inflation = us_inflation_json['observations']
us_inflation_df = pd.DataFrame(us_inflation).sort_values(by=['date'],ascending=False).reset_index().iloc[:, [3, 4]].rename(columns={"date": "Date", "value":"US_Inflation_Rate"})
us_inflation_df['Date'] = pd.to_datetime(us_inflation_df['Date'], format='%Y-%m-%d')
us_inflation_df['US_Inflation_Rate'] = us_inflation_df['US_Inflation_Rate'].astype(float)
#us_inflation_df.head(10)

us_inflation_sdf = spark.createDataFrame(us_inflation_df)
us_inflation_sdf.write.format("Parquet").mode("append").saveAsTable("Louis.us_inflation_table")

# COMMAND ----------

#US_Real GDP
us_rgdp_series_id = "GDPC1"

us_rgdp_url = f"https://api.stlouisfed.org/fred/series/observations?series_id={us_rgdp_series_id}&api_key={fred_api_key}&file_type=json"
us_rgdp_r = requests.get(us_rgdp_url)
us_rgdp_json = us_rgdp_r.json()
us_rgdp = us_rgdp_json['observations']
us_rgdp_df = pd.DataFrame(us_rgdp).sort_values(by=['date'],ascending=False).reset_index().iloc[:, [3, 4]].rename(columns={"date": "Date", "value":"US_Real_GDP(B)"})
us_rgdp_df['Date'] = pd.to_datetime(us_rgdp_df['Date'], format='%Y-%m-%d')
us_rgdp_df['US_Real_GDP(B)'] = us_rgdp_df['US_Real_GDP(B)'].astype(float)
#us_rgdp_df.head(10)

us_rgdp_sdf = spark.createDataFrame(us_rgdp_df)
us_rgdp_sdf.write.format("Parquet").mode("append").saveAsTable("Louis.us_rgdp_table")

# COMMAND ----------

#US_UNemployment_Rate
us_unemployment_id = "UNRATE"

us_unemployment_url = f"https://api.stlouisfed.org/fred/series/observations?series_id={us_unemployment_id}&api_key={fred_api_key}&file_type=json"
us_unemployment_r = requests.get(us_unemployment_url)
us_unemployment_json = us_unemployment_r.json()
us_unemployment = us_unemployment_json['observations']
us_unemployment_df = pd.DataFrame(us_unemployment).sort_values(by=['date'],ascending=False).reset_index().iloc[:, [3, 4]].rename(columns={"date": "Date", "value":"US_Unemployment_Rate"})
us_unemployment_df['Date'] = pd.to_datetime(us_unemployment_df['Date'], format='%Y-%m-%d')
us_unemployment_df['US_Unemployment_Rate'] = us_unemployment_df['US_Unemployment_Rate'].astype(float)
#us_unemployment_df.head(10)

us_unemployment_sdf = spark.createDataFrame(us_unemployment_df)
us_unemployment_sdf.write.format("Parquet").mode("append").saveAsTable("Louis.us_unemployment_table")

# COMMAND ----------

#US Government Public Debt
us_gov_debt = "GFDEBTN"

us_gov_debt_url = f"https://api.stlouisfed.org/fred/series/observations?series_id={us_gov_debt}&api_key={fred_api_key}&file_type=json"
us_gov_debt_r = requests.get(us_gov_debt_url)
us_gov_debt_json = us_gov_debt_r.json()
us_gov_debt = us_gov_debt_json['observations']
us_gov_debt_df = pd.DataFrame(us_gov_debt).sort_values(by=['date'],ascending=False).reset_index().iloc[:, [3, 4]].rename(columns={"date": "Date", "value":"US_Government_Debt(M)"})
us_gov_debt_df['Date'] = pd.to_datetime(us_gov_debt_df['Date'], format='%Y-%m-%d')
us_gov_debt_df['US_Government_Debt(M)'] = us_gov_debt_df['US_Government_Debt(M)'].astype(float)
#us_gov_debt_df.head(10)

us_gov_debt_sdf = spark.createDataFrame(us_gov_debt_df)
us_gov_debt_sdf.write.format("Parquet").mode("append").saveAsTable("Louis.us_gov_debt_table")

# COMMAND ----------

#EU Interest Rate
eu_interest_id = "ECBMRRFR"

eu_interest_url = f"https://api.stlouisfed.org/fred/series/observations?series_id={eu_interest_id}&api_key={fred_api_key}&file_type=json"
eu_interest_r = requests.get(eu_interest_url)
eu_interest_json = eu_interest_r.json()
eu_interest = eu_interest_json['observations']
eu_interest_df = pd.DataFrame(eu_interest).sort_values(by=['date'],ascending=False).reset_index().iloc[:, [3, 4]].rename(columns={"date": "Date", "value":"EU_Interest_Rate"})
eu_interest_df = eu_interest_df.replace('.',eu_interest_df[eu_interest_df['Date'] == '2000-06-27']['EU_Interest_Rate'].iloc[0])
eu_interest_df['Date'] = pd.to_datetime(eu_interest_df['Date'], format='%Y-%m-%d')
eu_interest_df['EU_Interest_Rate'] = eu_interest_df['EU_Interest_Rate'].astype(float)
#eu_interest_df

eu_interest_sdf = spark.createDataFrame(eu_interest_df)
eu_interest_sdf.write.format("Parquet").mode("append").saveAsTable("Louis.eu_interest_table")

# COMMAND ----------

#EU Inflation Rate
eu_inflation_id = "FPCPITOTLZGEMU"

eu_inflation_url = f"https://api.stlouisfed.org/fred/series/observations?series_id={eu_inflation_id}&api_key={fred_api_key}&file_type=json"
eu_inflation_r = requests.get(eu_inflation_url)
eu_inflation_json = eu_inflation_r.json()
eu_inflation = eu_inflation_json['observations']
eu_inflation_df = pd.DataFrame(eu_inflation).sort_values(by=['date'],ascending=False).reset_index().iloc[:, [3, 4]].rename(columns={"date": "Date", "value":"EU_Inflation_Rate"})
eu_inflation_df['Date'] = pd.to_datetime(eu_inflation_df['Date'], format='%Y-%m-%d')
eu_inflation_df['EU_Inflation_Rate'] = eu_inflation_df['EU_Inflation_Rate'].astype(float)
#eu_inflation_df.head(10)

eu_inflation_sdf = spark.createDataFrame(eu_inflation_df)
eu_inflation_sdf.write.format("Parquet").mode("append").saveAsTable("Louis.eu_inflation_table")

# COMMAND ----------

#EU Real GDP
eu_rgdp_series_id = "CLVMEURSCAB1GQEA19"

eu_rgdp_url = f"https://api.stlouisfed.org/fred/series/observations?series_id={eu_rgdp_series_id}&api_key={fred_api_key}&file_type=json"
eu_rgdp_r = requests.get(eu_rgdp_url)
eu_rgdp_json = eu_rgdp_r.json()
eu_rgdp = eu_rgdp_json['observations']
eu_rgdp_df = pd.DataFrame(eu_rgdp).sort_values(by=['date'],ascending=False).reset_index().iloc[:, [3, 4]].rename(columns={"date": "Date", "value":"EU_Real_GDP(M)"})
eu_rgdp_df['Date'] = pd.to_datetime(eu_rgdp_df['Date'], format='%Y-%m-%d')
eu_rgdp_df['EU_Real_GDP(M)'] = eu_rgdp_df['EU_Real_GDP(M)'].astype(float)
#eu_rgdp_df.head(10)

eu_rgdp_sdf = spark.createDataFrame(eu_rgdp_df)
eu_rgdp_sdf.write.format("Parquet").mode("append").saveAsTable("Louis.eu_rgdp_table")

# COMMAND ----------

#EU Unemployment_Rate
eu_unemployment_id = "LRHUTTTTEZM156S"

eu_unemployment_url = f"https://api.stlouisfed.org/fred/series/observations?series_id={eu_unemployment_id}&api_key={fred_api_key}&file_type=json"
eu_unemployment_r = requests.get(eu_unemployment_url)
eu_unemployment_json = eu_unemployment_r.json()
eu_unemployment = eu_unemployment_json['observations']
eu_unemployment_df = pd.DataFrame(eu_unemployment).sort_values(by=['date'],ascending=False).reset_index().iloc[:, [3, 4]].rename(columns={"date": "Date", "value":"EU_Unemployment_Rate"})
eu_unemployment_df['Date'] = pd.to_datetime(eu_unemployment_df['Date'], format='%Y-%m-%d')
eu_unemployment_df['EU_Unemployment_Rate'] = eu_unemployment_df['EU_Unemployment_Rate'].astype(float)
#eu_unemployment_df.head(10)

eu_unemployment_sdf = spark.createDataFrame(eu_unemployment_df)
eu_unemployment_sdf.write.format("Parquet").mode("append").saveAsTable("Louis.eu_unemployment_table")

# COMMAND ----------

#EU_Debet_Per till 2015
eu_deb_per_series_id = "GCDODTOTLGDZSEMU"

eu_deb_per_url = f"https://api.stlouisfed.org/fred/series/observations?series_id={eu_deb_per_series_id}&api_key={fred_api_key}&file_type=json"
eu_deb_per_r = requests.get(eu_deb_per_url)
eu_deb_per_json = eu_deb_per_r.json()
eu_deb_per = eu_deb_per_json['observations']
eu_deb_per_df = pd.DataFrame(eu_deb_per).sort_values(by=['date'],ascending=False).reset_index().iloc[:, [3, 4]].rename(columns={"date": "Date", "value":"EU_Deb%"})
eu_deb_per_df['Date'] = pd.to_datetime(eu_deb_per_df['Date'], format='%Y-%m-%d')
eu_deb_per_df['EU_Deb%'] = eu_deb_per_df['EU_Deb%'].astype(float)
#eu_deb_per_df.head(10)




# COMMAND ----------

#Eurostat_Debet_per_2011-2022

eustat_deb_per_url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/teina225?format=JSON&time=2011&time=2012&time=2013&time=2014&time=2015&time=2016&time=2017&time=2018&time=2019&time=2020&time=2021&time=2022&geo=EA19&unit=PC_GDP&na_item=GD&sector=S13&lang=en"

eustat_deb_per_r = requests.get(eustat_deb_per_url)
eustat_deb_per = eustat_deb_per_r.json()['value']
eustat_deb_per_df = pd.DataFrame(eustat_deb_per,index=[0]).T
eustat_deb_per_df['Date'] = range(2011,2023)
eustat_deb_per_df = eustat_deb_per_df[['Date',0]].rename(columns={"Date": "Date", 0:"EU_Deb%"}).sort_values(by='Date',ascending=False)
eustat_deb_per_df['Date'] = pd.to_datetime(eustat_deb_per_df['Date'], format='%Y')
eustat_deb_per_df['EU_Deb%'] = eustat_deb_per_df['EU_Deb%'].astype(float)
eustat_deb_per_df = eustat_deb_per_df[eustat_deb_per_df['Date'] > eu_deb_per_df.iloc[0]['Date']]

final_eu_deb_per_df = pd.concat([eustat_deb_per_df,eu_deb_per_df])
#final_eu_deb_per_df

final_eu_deb_per_sdf = spark.createDataFrame(final_eu_deb_per_df)
final_eu_deb_per_sdf.write.format("Parquet").mode("append").saveAsTable("Louis.final_eu_deb_per_table")

# COMMAND ----------

#EU GDP
eu_gdp_series_id = "EUNNGDP"

eu_gdp_url = f"https://api.stlouisfed.org/fred/series/observations?series_id={eu_gdp_series_id}&api_key={fred_api_key}&file_type=json"
eu_gdp_r = requests.get(eu_gdp_url)
eu_gdp_json = eu_gdp_r.json()
eu_gdp = eu_gdp_json['observations']
eu_gdp_df = pd.DataFrame(eu_gdp).sort_values(by=['date'],ascending=False).reset_index().iloc[:, [3, 4]].rename(columns={"date": "Date", "value":"EU_GDP(M)"})
eu_gdp_df['Date'] = pd.to_datetime(eu_gdp_df['Date'], format='%Y-%m-%d')
eu_gdp_df['EU_GDP(M)'] = eu_gdp_df['EU_GDP(M)'].astype(float)
#eu_gdp_df.head(10)

eu_gdp_sdf = spark.createDataFrame(eu_gdp_df)
eu_gdp_sdf.write.format("Parquet").mode("append").saveAsTable("Louis.eu_gdp_table1")

# COMMAND ----------

#Commodities_Index_Gold
tickerSymbol = 'GC=F'

# Get data on this ticker
tickerData = yf.Ticker(tickerSymbol)

# Get the historical prices for this ticker
gold_df = tickerData.history(period='1d', start='1999-1-1').reset_index().sort_values(by='Date',ascending=False)[['Date','Close']].rename(columns={'Date':'Date','Close':'Commodities_Index_Gold'})
gold_df['Date'] = pd.to_datetime(gold_df['Date'], format='%Y-%m-%d').dt.strftime('%Y-%m-%d')
gold_df['Commodities_Index_Gold'] = gold_df['Commodities_Index_Gold'].astype(float)

# See your data
#gold_df
gold_sdf = spark.createDataFrame(gold_df)
gold_sdf.write.format("Parquet").mode("append").saveAsTable("Louis.gold_table1")

# COMMAND ----------

#Commodities_Crude Oil
tickerSymbol = 'CL=F'

# Get data on this ticker
tickerData = yf.Ticker(tickerSymbol)

# Get the historical prices for this ticker
oil_df = tickerData.history(period='1d', start='1999-1-1').reset_index().sort_values(by='Date',ascending=False)[['Date','Close']].rename(columns={'Date':'Date','Close':'Commodities_Crude_Oil'})
oil_df['Date'] = pd.to_datetime(oil_df['Date'], format='%Y-%m-%d').dt.strftime('%Y-%m-%d')
oil_df['Commodities_Crude_Oil'] = oil_df['Commodities_Crude_Oil'].astype(float)

# See your data
#oil_df

oil_sdf = spark.createDataFrame(oil_df)
oil_sdf.write.format("Parquet").mode("append").saveAsTable("Louis.oil_table")

# COMMAND ----------

# Global Economic Policy Uncertainty Index
policy_uncer_idx_series_id = "GEPUCURRENT"

policy_uncer_idx_url = f"https://api.stlouisfed.org/fred/series/observations?series_id={policy_uncer_idx_series_id}&api_key={fred_api_key}&file_type=json"
policy_uncer_idx_r = requests.get(policy_uncer_idx_url)
policy_uncer_idx_json = policy_uncer_idx_r.json()
policy_uncer_idx = policy_uncer_idx_json['observations']
policy_uncer_idx_df = pd.DataFrame(policy_uncer_idx).sort_values(by=['date'],ascending=False).reset_index().iloc[:, [3, 4]].rename(columns={"date": "Date", "value":"Global_Economic_Policy_Uncertainty_Index"})
policy_uncer_idx_df['Date'] = pd.to_datetime(policy_uncer_idx_df['Date'], format='%Y-%m-%d')
policy_uncer_idx_df['Global_Economic_Policy_Uncertainty_Index'] = policy_uncer_idx_df['Global_Economic_Policy_Uncertainty_Index'].astype(float)
#print(policy_uncer_idx_df)

policy_uncer_idx_sdf = spark.createDataFrame(policy_uncer_idx_df)
policy_uncer_idx_sdf.write.format("Parquet").mode("append").saveAsTable("Louis.policy_uncer_idx_table")

# COMMAND ----------

# Volatility Index
volat_idx_series_id = "VIXCLS"

volat_idx_url = f"https://api.stlouisfed.org/fred/series/observations?series_id={volat_idx_series_id}&api_key={fred_api_key}&file_type=json"
volat_idx_r = requests.get(volat_idx_url)
volat_idx_json = volat_idx_r.json()
volat_idx = volat_idx_json['observations']
volat_idx_df = pd.DataFrame(volat_idx).sort_values(by=['date'],ascending=False).reset_index().iloc[:, [3, 4]].rename(columns={"date": "Date", "value":"Volatility_Index"})
volat_idx_df = volat_idx_df.replace('.',np.NaN)
volat_idx_df['Date'] = pd.to_datetime(volat_idx_df['Date'], format='%Y-%m-%d')
volat_idx_df['Volatility_Index'] = volat_idx_df['Volatility_Index'].astype(float)
#print(volat_idx_df)

volat_idx_sdf = spark.createDataFrame(volat_idx_df)
volat_idx_sdf.write.format("Parquet").mode("append").saveAsTable("Louis.volat_idx_table")

# COMMAND ----------

# Consumer Sentiment
consum_confid_series_id = "UMCSENT"

consum_confid_url = f"https://api.stlouisfed.org/fred/series/observations?series_id={consum_confid_series_id}&api_key={fred_api_key}&file_type=json"
consum_confid_r = requests.get(consum_confid_url)
consum_confid_json = consum_confid_r.json()
consum_confid = consum_confid_json['observations']
consum_confid_df = pd.DataFrame(consum_confid).sort_values(by=['date'],ascending=False).reset_index().iloc[:, [3, 4]].rename(columns={"date": "Date", "value":"Consumer_Sentiment"})
consum_confid_df = consum_confid_df.replace('.',np.NaN)
consum_confid_df['Date'] = pd.to_datetime(consum_confid_df['Date'], format='%Y-%m-%d')
consum_confid_df['Consumer_Sentiment'] = consum_confid_df['Consumer_Sentiment'].astype(float)
#print(consum_confid_df)

consum_confid_sdf = spark.createDataFrame(consum_confid_df)
consum_confid_sdf.write.format("Parquet").mode("append").saveAsTable("Louis.consum_confid_table")

# COMMAND ----------


