from pyspark.sql import SparkSession
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from io import StringIO

def run_spark(ti):
    filename = ti.xcom_pull(key='filename', task_ids=['getData'])[0]
    dataset = r'/opt/airflow/sparkFiles/parsedData.csv'
    
    spark = SparkSession \
        .builder \
        .appName("Pyspark_transform") \
        .getOrCreate() 
        
    df = spark.read.csv(dataset,
                        header='true',
                        inferSchema='true',
                        ignoreLeadingWhiteSpace=True,
                        ignoreTrailingWhiteSpace=True)
    df.createOrReplaceTempView("raw_data")
    
    transform_query = '''
    SELECT 
       timepoint AS forecast_time_diff
     , CAST(unix_timestamp(CAST(init as string), "yyyyMMddHH") as timestamp) AS forecast_time
     , temperature
     , CASE WHEN prec_type = 'none' THEN NULL
       ELSE prec_type END AS prec_type
     , CASE WHEN prec_amount = 0 THEN NULL
       ELSE prec_amount END AS prec_amount
     , CASE WHEN snow_depth = 0 THEN NULL
       ELSE snow_depth END AS snow_depth
     , cloud_cover
     , wind_direction
     , wind_speed
    FROM raw_data
    '''
    
    df= spark.sql(transform_query)

    df.show(10)
    
    csv_buffer = StringIO()
    df.toPandas().to_csv(csv_buffer)
    hook = S3Hook()
    hook.load_string (string_data = csv_buffer.getvalue(),
                    key = filename,
                    bucket_name = 'meteo-data-transformed',
                    replace = True
                    )

