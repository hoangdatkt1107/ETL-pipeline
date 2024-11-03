import findspark
findspark.init()
import pyspark
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from datetime import date
from datetime import timedelta
from pyspark.errors import AnalysisException
import os 

spark = SparkSession.builder.config("spark.driver.memory","16g").appName("ETL").getOrCreate()

def transform_data(df):
    data = df.select('_source.AppName','_source.Contract','_source.Mac','_source.TotalDuration')
    data = data.withColumn('category',when((col('AppName') == 'KPLUS') | (col('AppName') == 'CHANNEL') | (col('AppName') == 'DSHD'),'Truyền Hình').when((col('AppName') == 'FIMS') | (col('AppName') == 'RELAX') | (col('AppName') == 'CHILD') | (col('AppName') == 'VOD') | 
    (col('AppName') == 'SPORT'),'Phim Truyện').otherwise('other'))
    data = data.drop('Mac')
    return data

def pivot_table(data):
    data = data.groupby('Contract','AppName').agg(sum('TotalDuration').alias('TotalDuration'))
    data = data.groupby('Contract').pivot('Appname').sum('TotalDuration')
    data = data.fillna(0)
    return data

def activeness(df):
    df = df.select('_source.AppName','_source.Contract','_source.Mac','_source.TotalDuration','date')
    df = df.groupBy('Contract', 'date').agg(sum('TotalDuration').alias('TotalDuration'))
    df = df.groupBy('Contract').agg(sum(when(col('TotalDuration') > 0, 1).otherwise(0)).alias('activeness'))
    return df

def most_watch(data):
    columns = ['APP', 'BHD', 'CHANNEL', 'CHILD', 'FIMS', 'KPLUS', 'RELAX', 'SPORT', 'VOD']
    data = data.withColumn('max_value', greatest(*[col(c) for c in columns]))
    most_view_expr = None
    for c in columns:
        if most_view_expr is None:
            most_view_expr = when(col(c) == col('max_value'), c)
        else:
            most_view_expr = most_view_expr.when(col(c) == col('max_value'), c)
    data = data.withColumn('most_view', most_view_expr)
    data = data.withColumn('most_view', when(col('max_value') == 0, None).otherwise(col('most_view')))
    return data

def customer_taste(data):
    conditions = [
    when(col('APP') != 0, lit('APP')),
    when(col('BHD') != 0, lit('BHD')),
    when(col('CHANNEL') != 0, lit('CHANNEL')),
    when(col('CHILD') != 0, lit('CHILD')),
    when(col('FIMS') != 0, lit('FIMS')),
    when(col('KPLUS') != 0, lit('KPLUS')),
    when(col('RELAX') != 0, lit('RELAX')),
    when(col('SPORT') != 0, lit('SPORT')),
    when(col('VOD') != 0, lit('VOD'))]

    data = data.withColumn('customer_taste',concat_ws('-',*[cond for cond in conditions]))
    return data

def process_all_data(df):
    all_data = transform_data(df)
    print('---The data has been transformed---')
    all_data = pivot_table(all_data)
    print('---The table has been pivoted---')
    all_data = most_watch(all_data)
    print('---most watch has been done---')
    all_data = customer_taste(all_data)
    print('---customer taste has been done---')
    df1 = activeness(df)
    print('---customer activeness has been done---')
    
    all_data = all_data.join(df1,on ='Contract',how ='inner')
    return all_data

def date_range(from_date:date,to_date:date):
    date_r =[]
    for i in range((to_date - from_date).days + 1):
        date_r.append(from_date + timedelta(days=i))
    return date_r

def etl_data(input_path):
    date_r = date_range(from_date,to_date)
    df_final = None
    for i in date_r:
        try:
            input_data = f"{input_path}/{i.strftime('%Y%m%d')}.json"
            print(f'read file {input_data}')
            df = spark.read.format('json').option('header','true').load(input_data)
            df = df.withColumn('date',lit(i))
            df = df.limit(100)
            if df_final is None:
                df_final = df
            else:
                df_final = df_final.union(df)
        except AnalysisException as errors:
            print(f'errors-{errors}')
    df_final = process_all_data(df_final)
    return df_final

if __name__ =='__main__':
    from_date = date(2022,4,1)
    to_date = date(2022,4,30)
    input_path = '/Users/hoangdat/Data/Dataset/dataset/log_content'
    final = etl_data(input_path)
    final.show(10)
    print('ETL_Done')