import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import date, timedelta
from dotenv import load_dotenv
import os
import glob
import logging

# Initialize Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Load environment variables from .env file
load_dotenv()

# Get environment variables
mongodb_uri = os.getenv('MONGODB_URI')
postgresql_jdbc_url = os.getenv('POSTGRESQL_JDBC_URL')
spark_driver_memory = os.getenv('SPARK_DRIVER_MEMORY', '16g')
spark_driver_cores = os.getenv('SPARK_DRIVER_CORES', '4')
app_name = os.getenv('APP_NAME', 'ETL Pipeline')
jars_dir = os.getenv('SPARK_JARS_DIR', './jars')

# Get all JAR files in the jars directory
jar_files = glob.glob(os.path.join(jars_dir, "*.jar"))
spark_jars = ",".join(jar_files)

# Initialize Spark session
spark = SparkSession.builder \
    .appName(app_name) \
    .config("spark.mongodb.input.uri", mongodb_uri) \
    .config("spark.mongodb.output.uri", mongodb_uri) \
    .config("spark.jars", spark_jars) \
    .config("spark.driver.memory", spark_driver_memory) \
    .config("spark.driver.cores", spark_driver_cores) \
    .getOrCreate()

# Transformation functions
def transform_data(df):
    data = df.select('_source.AppName','_source.Contract','_source.Mac','_source.TotalDuration')
    data = data.withColumn('category', 
        when((col('AppName') == 'KPLUS') | (col('AppName') == 'CHANNEL') | (col('AppName') == 'DSHD'), 'Truyền Hình')
        .when((col('AppName') == 'FIMS') | (col('AppName') == 'RELAX') | (col('AppName') == 'CHILD') | (col('AppName') == 'VOD') | (col('AppName') == 'SPORT'), 'Phim Truyện')
        .otherwise('other'))
    data = data.drop('Mac')
    return data

def pivot_table(data):
    data = data.groupby('Contract', 'AppName').agg(sum('TotalDuration').alias('TotalDuration'))
    data = data.groupby('Contract').pivot('AppName').sum('TotalDuration')
    return data.fillna(0)

def activeness(df):
    df = df.select('_source.AppName', '_source.Contract', '_source.Mac', '_source.TotalDuration')
    df = df.groupBy('Contract').agg(sum('TotalDuration').alias('TotalDuration'))
    df = df.groupBy('Contract').agg(sum(when(col('TotalDuration') > 0, 1).otherwise(0)).alias('activeness'))
    return df

def most_watch(data):
    columns = ['APP', 'BHD', 'CHANNEL', 'CHILD', 'FIMS', 'KPLUS', 'RELAX', 'SPORT', 'VOD']
    existing_columns = [c for c in columns if c in data.columns]
    if not existing_columns:
        return data
    
    data = data.withColumn('max_value', greatest(*[col(c) for c in existing_columns]))
    most_view_expr = None
    for c in existing_columns:
        if most_view_expr is None:
            most_view_expr = when(col(c) == col('max_value'), c)
        else:
            most_view_expr = most_view_expr.when(col(c) == col('max_value'), c)
    data = data.withColumn('most_view', most_view_expr)
    data = data.withColumn('most_view', when(col('max_value') == 0, None).otherwise(col('most_view')))
    return data

def customer_taste(data):
    columns = ['APP', 'BHD', 'CHANNEL', 'CHILD', 'FIMS', 'KPLUS', 'RELAX', 'SPORT', 'VOD']
    existing_columns = [c for c in columns if c in data.columns]
    conditions = [when(col(c) != 0, lit(c)) for c in existing_columns]
    if not conditions:
        return data
    return data.withColumn('customer_taste', concat_ws('-', *[cond for cond in conditions]))

def add_date(data):
    start_date = "2020-01-01"
    window_spec = Window.orderBy(lit(1))
    data = data.withColumn("row_number", row_number().over(window_spec))
    data = data.withColumn("date", date_add(lit(start_date), col("row_number") - 1))
    return data.drop("row_number")

# Main ETL process
def process_all_data(df):
    all_data = transform_data(df)
    logger.info('Data has been transformed')
    
    all_data = pivot_table(all_data)
    logger.info('Pivot table has been created')
    
    all_data = most_watch(all_data)
    logger.info('Most watched app has been calculated')
    
    all_data = customer_taste(all_data)
    logger.info('Customer taste has been determined')
    
    df1 = activeness(df)
    logger.info('Activeness has been calculated')
    
    all_data = all_data.join(df1, on='Contract', how='inner')
    all_data = add_date(all_data)
    return all_data

def etl_data():
    df_final = spark.read.format("mongo").load()
    return process_all_data(df_final)

def connect_cloudsql(final):
    final.write \
         .format("jdbc") \
         .option("url", postgresql_jdbc_url) \
         .option("dbtable", "random_data_table") \
         .option("driver", "org.postgresql.Driver") \
         .mode("overwrite") \
         .save()
    logger.info('Data written to Cloud SQL successfully')

if __name__ == '__main__':
    final_data = etl_data()
    connect_cloudsql(final_data)
    final_data.show(10)
    logger.info('ETL process completed')
