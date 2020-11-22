from pyspark.sql.types import *
from pyspark.sql import functions as fn
from pyspark.sql.functions import *
import immigration_udf
import logging
import configparser
from pathlib import Path
from pyspark.sql.types import StringType,TimestampType
from pyspark.sql.functions import udf
from time import strptime
from datetime import datetime, timedelta
from pyspark.sql import types as T
from pyspark.sql.functions import trim,regexp_replace
import configparser
from pathlib import Path
from pyspark.sql.functions import datediff

class ImmigrationDatasetTransform:

    """
    This class performs transformation operations on the dataset.
    """


def __init__():
    
        spark=SparkSession.builder.master('yarn').appName("goodreads") \
                          .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11") \
                          .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.2") \
                          .enableHiveSupport().getOrCreate()
        load_path = 's3a://udacitycapstoneproject/immigration_files'
        save_path = 's3a://udacitycapstoneproject/immigration_processed_files'
        

def convert_datetime(x):
    try:
        start = datetime(1960, 1, 1)
        return start + timedelta(days=int(x))
    except:
        return None
udf_datetime_from_sas = udf(lambda x: convert_datetime(x), T.DateType())

def transform_immigration_dataset(self):
    
    df_spark=spark.read.parquet('../workspace/immigration_files/sas_data/*.parquet')

    df_immi_fact=df_spark.fillna('Not available', subset=['gender','occup','i94addr']) 

    df_date=df_immi_fact.withColumn("arrival_date", udf_datetime_from_sas("arrdate")).withColumn("departure_date", udf_datetime_from_sas("depdate"))

    df_date=df_date.withColumn("cicid", df_date["cicid"].cast(IntegerType())) \
                    .withColumn("i94yr",df_date["i94yr"].cast(IntegerType())) \
                    .withColumn("i94mon",df_date["i94mon"].cast(IntegerType())) \
                    .withColumn("i94cit",df_date["i94cit"].cast(IntegerType())) \
                    .withColumn("i94res",df_date["i94res"].cast(IntegerType())) \
                    .withColumn("i94mode",df_date["i94mode"].cast(IntegerType())) \
                    .withColumn("i94addr",df_date["i94addr"])\
                    .withColumn("i94bir",df_date["i94bir"].cast(IntegerType()))\
                    .withColumn("i94visa",df_date["i94visa"].cast(IntegerType()))\
                    .withColumn("count",df_date["count"].cast(IntegerType()))\
                    .withColumn("biryear",df_date["biryear"].cast(IntegerType()))\
                    .withColumn("no_of_days_stayed", datediff(col("departure_date"),col("arrival_date")))     

    immigration=df_date.select('cicid','i94yr','i94mon',\
                               'i94cit','i94res','i94port','i94mode',\
                               'i94addr','i94bir','i94visa',\
                               'visapost','entdepa','biryear',\
                               'gender','airline','visatype','arrival_date',\
                               'departure_date','no_of_days_stayed')

    immigration.write.partitionBy("i94mon").mode("append").parquet(save_path +"/sasdata/")

def transform_airport_dataset(self):
       
        df_airport = spark.read.format("csv").option("header",True).load('../workspace/immigration_files/airport/airport-codes_csv.csv')

        not_null_iata_in_us_df = df_airport.where("iso_country = 'US' and iata_code is not null")

        not_null_iata_in_us_df = not_null_iata_in_us_df.withColumn("ident", trim(not_null_iata_in_us_df.ident)) \
                                                        .withColumn("type", trim(not_null_iata_in_us_df.type)) \
                                                        .withColumn("name", trim(not_null_iata_in_us_df.name)) \
                                                        .withColumn("elevation_ft", trim(not_null_iata_in_us_df.elevation_ft)) \
                                                        .withColumn("continent", trim(not_null_iata_in_us_df.continent)) \
                                                        .withColumn("iso_country", trim(not_null_iata_in_us_df.iso_country)) \
                                                        .withColumn("iso_region", trim(not_null_iata_in_us_df.iso_region)) \
                                                        .withColumn("municipality", trim(not_null_iata_in_us_df.municipality)) \
                                                        .withColumn("gps_code", trim(not_null_iata_in_us_df.gps_code)) \
                                                        .withColumn("iata_code", trim(not_null_iata_in_us_df.iata_code)) \
                                                        .withColumn("local_code", trim(not_null_iata_in_us_df.local_code)) \
                                                        .withColumn("coordinates", trim(not_null_iata_in_us_df.coordinates)) 


        not_null_iata_in_us_df=not_null_iata_in_us_df.withColumn("name", regexp_replace('name', "\\", ""))
        not_null_iata_in_us_df.write\
                              .csv(path = save_path + '/airport/',mode='overwrite', header=True)


def transform_us_demo_dataset(self):
        df_us = spark.read.options(header='True', inferSchema='True', delimiter=';') \
              .csv("../workspace/immigration_files/demo/us-cities-demographics.csv")

        df_us = df_us.withColumnRenamed("Median Age", "Median_Age") \
                    .withColumnRenamed("Male Population","Male_Population") \
                    .withColumnRenamed("Female Population", "Female_Population") \
                    .withColumnRenamed("Total Population", "Total_Population") \
                    .withColumnRenamed("Number of Veterans", "Number_of_Veterans") \
                    .withColumnRenamed("Foreign-born", "Foreign_born") \
                    .withColumnRenamed("Average Household Size", "Average_Household_Size") \
                    .withColumnRenamed("State Code", "State_Code")

        df_us = df_us.withColumn("City", trim(df_us.City)) \
                    .withColumn("State", trim(df_us.State)) \
                    .withColumn("Median_Age", trim(df_us.Median_Age)) \
                    .withColumn("Male_Population", trim(df_us.Male_Population)) \
                    .withColumn("Female_Population", trim(df_us.Female_Population)) \
                    .withColumn("Total_Population", trim(df_us.Total_Population)) \
                    .withColumn("Number_of_Veterans", trim(df_us.Number_of_Veterans)) \
                    .withColumn("Foreign_born", trim(df_us.Foreign_born)) \
                    .withColumn("Average_Household_Size", trim(df_us.Average_Household_Size)) \
                    .withColumn("State_Code", trim(df_us.State_Code)) \
                    .withColumn("Race", trim(df_us.Race)) \
                    .withColumn("Count", trim(df_us.Count))    

        df_us.write\
        .csv(path = save_path + '/demo/',mode='overwrite', header=True)



def transform_country(self):
        schema=StructType([StructField("value", IntegerType(), True),
                           StructField("i94cntyl", StringType(), True)
                          ])
        df_country = spark.read.format("csv").options(header='true').schema(schema).load('../workspace/immigration_files/rescitycntry/I94City_Res.csv')
        df_country = df_country.withColumn('i94cntyl', regexp_replace('i94cntyl', "\'", ""))

        df_country = df_country.withColumnRenamed("value", "country_id") \
                               .withColumnRenamed("i94cntyl", "country")

        df_country.write\
        .csv(path = save_path + '/rescitycntry/',mode='overwrite',header=True)
        
def transform_state(self):
        schema=StructType([StructField("value", StringType(), True),
                           StructField("i94addrl", StringType(), True)
                          ])

        df_Address = spark.read.format("csv").options(header='true').schema(schema).load('../workspace/immigration_files/addrstate/I94ADDR_State.csv')
        newDf = df_Address.withColumn('value',regexp_replace('value',"\'","")) \
                          .withColumn('i94addrl', regexp_replace('i94addrl', "\'", ""))

        df_State = newDf.withColumnRenamed("value", "state_id") \
                        .withColumnRenamed("i94addrl", "state_name")

        df_State.write\
        .csv(path = save_path + '/addrstate/I94ADDR_State.csv',mode='overwrite',header=True)

def transform_port(self):

        schema=StructType([StructField("value", StringType(), True),
                                 StructField("i94prtl", StringType(), True)
                                 ])
       
        df_port = spark.read.format("csv").options(header='true').schema(schema).load('../workspace/immigration_files/port/I94_Port.csv')
        df_port = df_port.withColumn('value', regexp_replace('value', "\'", "")) \
                         .withColumn('i94prtl', regexp_replace('i94prtl', "\'", ""))

        df_port = df_port.withColumnRenamed("value", "port_id") \
                          .withColumnRenamed("i94prtl", "port_name")

        df_port.write\
               .csv(path = save_path + '/port/I94_Port.csv',mode='overwrite',header=True)