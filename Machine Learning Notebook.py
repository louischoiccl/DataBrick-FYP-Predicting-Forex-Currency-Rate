# Databricks notebook source
import pandas as pd
import numpy as np
import datetime
import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix
from sklearn.preprocessing import OneHotEncoder
import numpy as np
from imblearn.over_sampling import RandomOverSampler
from sklearn.metrics import precision_recall_fscore_support

# COMMAND ----------

predict_eurusd_table = spark.sql("SELECT * FROM Louis.final_sampled_data_table")
predict_eurusd_df = predict_eurusd_table.toPandas().sort_values(by='Date',ascending=False).set_index('Date')
predict_eurusd_df

# COMMAND ----------

modeling_df = predict_eurusd_df[['EUR/USD','Commodities_Crude_Oil','US_Unemployment_Rate','Commodities_Index_Gold','EU_Unemployment_Rate','Consumer_Sentiment','US_Dollar_Index']]
modeling_df = modeling_df.reset_index()
modeling_df

# COMMAND ----------

lag_lead_modeling_df1 = modeling_df.copy()
lag_lead_modeling_df1['Commodities_Index_Gold_lead10'] = lag_lead_modeling_df1['US_Unemployment_Rate'].shift(-10)
lag_lead_modeling_df1['Commodities_Crude_Oil_lag10'] = lag_lead_modeling_df1['US_Unemployment_Rate'].shift(10)
lag_lead_modeling_df1=lag_lead_modeling_df1.dropna()
lag_lead_modeling_df1=lag_lead_modeling_df1.drop(columns=['Commodities_Crude_Oil', 'Commodities_Index_Gold'])
lag_lead_modeling_df1

# COMMAND ----------

lag_lead_modeling_df2 = modeling_df.copy()
lag_lead_modeling_df2['Commodities_Index_Gold_lead5'] = lag_lead_modeling_df2['US_Unemployment_Rate'].shift(-5)
lag_lead_modeling_df2['Commodities_Crude_Oil_lag5'] = lag_lead_modeling_df2['US_Unemployment_Rate'].shift(5)
lag_lead_modeling_df2=lag_lead_modeling_df2.dropna()
lag_lead_modeling_df2=lag_lead_modeling_df2.drop(columns=['Commodities_Crude_Oil', 'Commodities_Index_Gold'])
lag_lead_modeling_df2

# COMMAND ----------

# MAGIC %md
# MAGIC Withod Ladging Leading

# COMMAND ----------

up_down_predict_modeling_df = modeling_df.copy().sort_values(by='Date',ascending=True).reset_index(drop = True)
up_down_predict_modeling_df

# COMMAND ----------

up_down_predict_modeling_df['UpDown'] = (up_down_predict_modeling_df['EUR/USD'].diff() > 0).astype(int)
up_down_predict_modeling_df = up_down_predict_modeling_df.dropna()

# COMMAND ----------

up_down_predict_modeling_df

# COMMAND ----------

up_down_predict_modeling_df['Month'] = range(1,len(up_down_predict_modeling_df)+1)

# COMMAND ----------

up_down_predict_modeling_df

# COMMAND ----------

y = up_down_predict_modeling_df['UpDown']
X = up_down_predict_modeling_df.drop(['EUR/USD','UpDown','Date','Month'], axis=1)
train_size = int(len(up_down_predict_modeling_df) * 0.8)

# COMMAND ----------

X

# COMMAND ----------

#X_train, X_test, y_train, y_test = train_test_split(X, y)

# Split your features
X_train = X[:train_size]
X_test = X[train_size:]

# Split your target variable
y_train = y[:train_size]
y_test = y[train_size:]

X_train.shape, X_test.shape, y_train.shape, y_test.shape

# COMMAND ----------

model = RandomForestClassifier()


# COMMAND ----------

model.fit(X_train, y_train)

# COMMAND ----------

y_pred = model.predict(X_test)

# COMMAND ----------

y_pred

# COMMAND ----------

(y_pred == y_test).mean()

# COMMAND ----------

precision, recall, fscore, support = precision_recall_fscore_support(y_test, y_pred)

precision[1], recall[1], fscore[1]

# COMMAND ----------

confusion_matrix(y_test, y_pred)

# COMMAND ----------

# MAGIC %md
# MAGIC Lag_Lead_5 Period

# COMMAND ----------

up_down_predict_lag_lead_modeling_df2 = lag_lead_modeling_df2.copy().sort_values(by='Date',ascending=True).reset_index(drop = True)
up_down_predict_lag_lead_modeling_df2['UpDown'] = (up_down_predict_lag_lead_modeling_df2['EUR/USD'].diff() > 0).astype(int)
up_down_predict_lag_lead_modeling_df2 = up_down_predict_lag_lead_modeling_df2.dropna()
up_down_predict_lag_lead_modeling_df2['Month'] = range(1,len(up_down_predict_lag_lead_modeling_df2)+1)
up_down_predict_lag_lead_modeling_df2

# COMMAND ----------

y_5 = up_down_predict_lag_lead_modeling_df2['UpDown']
X_5 = up_down_predict_lag_lead_modeling_df2.drop(['EUR/USD','UpDown','Date','Month'], axis=1)
train_size = int(len(up_down_predict_lag_lead_modeling_df2) * 0.8)

# COMMAND ----------

#X_train, X_test, y_train, y_test = train_test_split(X, y)

# Split your features
X_train_5 = X_5[:train_size]
X_test_5 = X_5[train_size:]

# Split your target variable
y_train_5 = y_5[:train_size]
y_test_5 = y_5[train_size:]

X_train_5.shape, X_test_5.shape, y_train_5.shape, y_test_5.shape

# COMMAND ----------

model2 = RandomForestClassifier()

# COMMAND ----------

model2.fit(X_train_5, y_train_5)

# COMMAND ----------

y_pred_5 = model2.predict(X_test_5)

# COMMAND ----------

(y_pred_5 == y_test_5).mean()

# COMMAND ----------

precision, recall, fscore, support = precision_recall_fscore_support(y_test_5, y_pred_5)

precision[1], recall[1], fscore[1]

# COMMAND ----------

confusion_matrix(y_test_5, y_pred_5)

# COMMAND ----------

# MAGIC %md
# MAGIC Lag_lead_10 period

# COMMAND ----------

up_down_predict_lag_lead_modeling_df1 = lag_lead_modeling_df1.copy().sort_values(by='Date',ascending=True).reset_index(drop = True)
up_down_predict_lag_lead_modeling_df1['UpDown'] = (up_down_predict_lag_lead_modeling_df1['EUR/USD'].diff() > 0).astype(int)
up_down_predict_lag_lead_modeling_df1 = up_down_predict_lag_lead_modeling_df1.dropna()
up_down_predict_lag_lead_modeling_df1['Month'] = range(1,len(up_down_predict_lag_lead_modeling_df1)+1)
up_down_predict_lag_lead_modeling_df1

# COMMAND ----------

y_10 = up_down_predict_lag_lead_modeling_df1['UpDown']
X_10 = up_down_predict_lag_lead_modeling_df1.drop(['EUR/USD','UpDown','Date','Month'], axis=1)
train_size = int(len(up_down_predict_lag_lead_modeling_df1) * 0.8)

# COMMAND ----------

#X_train, X_test, y_train, y_test = train_test_split(X, y)

# Split your features
X_train_10 = X_10[:train_size]
X_test_10 = X_10[train_size:]

# Split your target variable
y_train_10 = y_10[:train_size]
y_test_10 = y_10[train_size:]

X_train_10.shape, X_test_10.shape, y_train_10.shape, y_test_10.shape

# COMMAND ----------

model2 = RandomForestClassifier()

# COMMAND ----------

model2.fit(X_train_10, y_train_10)
y_pred_10 = model2.predict(X_test_10)
(y_pred_10 == y_test_10).mean()

# COMMAND ----------

precision, recall, fscore, support = precision_recall_fscore_support(y_test_10, y_pred_10)

precision[1], recall[1], fscore[1]

# COMMAND ----------

confusion_matrix(y_test_10, y_pred_10)

# COMMAND ----------


