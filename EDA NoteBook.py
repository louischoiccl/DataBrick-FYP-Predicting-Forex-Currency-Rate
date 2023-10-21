# Databricks notebook source
import pandas as pd
import numpy as np
import datetime
import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

predict_eurusd_table = spark.sql("SELECT * FROM Louis.final_sampled_data_table")
predict_eurusd_df = predict_eurusd_table.toPandas().sort_values(by='Date',ascending=False).set_index('Date')
predict_eurusd_df

# COMMAND ----------

predict_eurusd_df.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC Distribution: Show the range of Value how often they occur
# MAGIC

# COMMAND ----------

#Histogram to see EUR/USD distribution
plt.hist(predict_eurusd_df['EUR/USD'],bins=45)
plt.title("EUR/USD Distribution")
plt.xlabel("EUR/USD")

# COMMAND ----------

#Histogram to see EUR/USD distribution
plt.hist(predict_eurusd_df['US_Dollar_Index'],bins=35)
plt.title("US_Dollar_Index Distribution")
plt.xlabel("US_Dollar_Index")

# COMMAND ----------

# MAGIC %md
# MAGIC Correlation Analysis

# COMMAND ----------

predict_eurusd_df.corr()['EUR/USD'].sort_values(ascending = False)

# COMMAND ----------

predict_eurusd_df.corr()[(predict_eurusd_df.corr() > 0.4) | (predict_eurusd_df.corr() < -0.4)]

# COMMAND ----------

sns.heatmap(predict_eurusd_df.corr())

# COMMAND ----------

# MAGIC %md
# MAGIC Time Series Analysis

# COMMAND ----------

#Normalize
from sklearn.preprocessing import MinMaxScaler

scaler = MinMaxScaler()
to_scale = predict_eurusd_df.columns

# Apply scaler
normalized_predict_eurusd_df = predict_eurusd_df
normalized_predict_eurusd_df[to_scale] = scaler.fit_transform(predict_eurusd_df[to_scale])

plt.figure(figsize=(14,8))

# Replace 'Other_Variable' with your column
plt.plot(normalized_predict_eurusd_df.index, normalized_predict_eurusd_df['EUR/USD'], label='EUR/USD')
plt.plot(normalized_predict_eurusd_df.index, normalized_predict_eurusd_df['Commodities_Crude_Oil'], label='Commodities_Crude_Oil')

plt.title('Normalized EUR/USD Vs Commodities_Crude_Oil')
plt.xlabel('Date')
plt.ylabel('Normalized Value')
plt.legend()

plt.show()

# COMMAND ----------

plt.figure(figsize=(14,8))

# Replace 'Other_Variable' with your column
plt.plot(normalized_predict_eurusd_df.index, normalized_predict_eurusd_df['EUR/USD'], label='EUR/USD')
plt.plot(normalized_predict_eurusd_df.index, normalized_predict_eurusd_df['US_Dollar_Index'], label='US_Dollar_Index')

plt.title('Normalized EUR/USD Vs US_Dollar_Index')
plt.xlabel('Date')
plt.ylabel('Normalized Value')
plt.legend()

plt.show()

# COMMAND ----------

plt.figure(figsize=(14,8))

# Replace 'Other_Variable' with your column
plt.plot(normalized_predict_eurusd_df.index, normalized_predict_eurusd_df['EUR/USD'], label='EUR/USD')
plt.plot(normalized_predict_eurusd_df.index, normalized_predict_eurusd_df['Consumer_Sentiment'], label='Consumer_Sentiment')

plt.title('Normalized EUR/USD Vs Consumer_Sentiment')
plt.xlabel('Date')
plt.ylabel('Normalized Value')
plt.legend()

plt.show()

# COMMAND ----------

plt.figure(figsize=(14,8))

# Replace 'Other_Variable' with your column
plt.plot(normalized_predict_eurusd_df.index, normalized_predict_eurusd_df['EUR/USD'], label='EUR/USD')
plt.plot(normalized_predict_eurusd_df.index, normalized_predict_eurusd_df['Commodities_Index_Gold'], label='Commodities_Index_Gold')

plt.title('Normalized EUR/USD Vs Commodities_Index_Gold')
plt.xlabel('Date')
plt.ylabel('Normalized Value')
plt.legend()

plt.show()

# COMMAND ----------

plt.figure(figsize=(14,8))

# Replace 'Other_Variable' with your column
plt.plot(normalized_predict_eurusd_df.index, normalized_predict_eurusd_df['EUR/USD'], label='EUR/USD')
plt.plot(normalized_predict_eurusd_df.index, normalized_predict_eurusd_df['EU_Unemployment_Rate'], label='EU_Unemployment_Rate')

plt.title('Normalized EUR/USD Vs EU_Unemployment_Rate')
plt.xlabel('Date')
plt.ylabel('Normalized Value')
plt.legend()

plt.show()

# COMMAND ----------

sns.scatterplot(data=predict_eurusd_df, x="EUR/USD", y="US_Dollar_Index").set(title='Scatter Plot EUR/USD vs US Dollar Index')

# COMMAND ----------

sns.scatterplot(data=predict_eurusd_df, x="EUR/USD", y="US_Inflation_Rate").set(title='Scatter Plot EUR/USD vs US_Inflation_Rate')

# COMMAND ----------

sns.scatterplot(data=predict_eurusd_df, x="EUR/USD", y="EU_Interest_Rate").set(title='Scatter Plot EUR/USD vs EU_Interest_Rate')
