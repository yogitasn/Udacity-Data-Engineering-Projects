from pyspark.sql.types import StringType
from pyspark.sql import functions as fn
import immigration_udf
import logging
import configparser
from pathlib import Path


logger = logging.getLogger(__name__)
config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/config.cfg"))


class ImmigrationDatasetTransform:

    """
    This class performs transformation operations on the dataset.
    1. Transform timestamp format, clean text part, remove extra spaces etc.
    2. Create a lookup dataframe which contains the id and the timestamp for the latest record.
    3. Join this lookup data frame with original dataframe to get only the latest records from the dataset.
    4. Save the dataset by repartitioning. Using gzip compression
    """


def __init__(self, spark):
    
        self._spark = spark
        self._load_path = 's3a://' + config.get('BUCKET', 'WORKING_ZONE')
        self._save_path = 's3a://' + config.get('BUCKET', 'PROCESSED_ZONE')


def transform_immigration_dataset(self):
    
    logging.debug("Inside transform immigration dataset module")
    df_spark=self._spark.read.format('com.github.saurfang.sas.spark') \
                       .load("s3a://udacitycapstoneproject/sasdata/*.parquet",header=True, mode='PERMISSIVE',inferSchema=True)
    df_immi_fact=df_spark.fillna('Not available', subset=['gender','occup','i94addr']) 
    df_date=df_immi_fact.withColumn("arrival_date", udf_datetime_from_sas("arrdate")) \
                        .withColumn("departure_date", udf_datetime_from_sas("depdate"))
    df_date=df_date.withColumn("cicid", df_date["cicid"].cast(IntegerType())) \
                    .withColumn("i94yr",df_date["i94yr"].cast(IntegerType())) \
                    .withColumn("i94mon",df_date["i94mon"].cast(IntegerType())) \
                    .withColumn("i94cit",df_date["i94cit"].cast(IntegerType())) \
                    .withColumn("i94res",df_date["i94res"].cast(IntegerType())) \
                    .withColumn("i94mode",df_date["i94mode"].cast(IntegerType())) \
                    .withColumn("i94addr",df_date["i94addr"]) \
                    .withColumn("i94bir",df_date["i94bir"].cast(IntegerType())) \
                    .withColumn("i94visa",df_date["i94visa"].cast(IntegerType())) \
                    .withColumn("count",df_date["count"].cast(IntegerType())) \
                    .withColumn("biryear",df_date["biryear"].cast(IntegerType())) \
                    .withColumn("no_of_days_stayed", datediff(col("departure_date"),col("arrival_date")))     
    
     immigration=df_date.select('cicid','i94yr','i94mon','i94cit','i94port',\
                                'i94mode','i94addr','i94bir','i94visa','visapost',\
                                'entdepa','biryear','gender','airline','visatype',\
                                'arrival_date','departure_date','no_of_days_stayed')

   
     immigration.write.partitionBy("i94yr").mode("append").parquet(self._save_path +"/immigration/")
    


def transform_airport_dataset(self):

        logging.debug("Inside transform airport dataset module")

        df_airport = self._spark.read \
                                .csv(self._load_path + 'airport/airport-codes_csv.csv', header=True, \
                                     mode = 'PERMISSIVE', inferSchema=True, quote = "\"", escape = "\"")
        not_null_iata_in_us_df = df_airport.where("iso_country = 'US' and iata_code is not null")
        
        not_null_iata_in_us_df.repartition(10)\
            .write\
            .csv(path = self._save_path + '/airport/', sep = '|', mode='overwrite', compression='gzip', header=True, timestampFormat = 'yyyy-MM-dd HH:mm:ss.SSS', quote = '"', escape = '"')


 
def transform_us_demo_dataset(self):
       logging.debug("Inside transform us demographics dataset module")
       df_us = self._spark.read.csv(self._load_path + 'demo/us-cities-demographics.csv', \ 
                                       header=True, mode='PERMISSIVE',\
                                       inferSchema=True, quote="\"", escape="\"")

 
        df_us_cities_demo=df_us.withColumn("City", split(col("col1"), ";").getItem(0)) \
             .withColumn("State",split(col("col1"), ";").getItem(1)) \
             .withColumn("Median_Age",split(col("col1"), ";").getItem(2).cast(FloatType())) \
             .withColumn("Male_Population",split(col("col1"), ";").getItem(3).cast(IntegerType())) \
             .withColumn("Female_Population",split(col("col1"), ";").getItem(4).cast(IntegerType())) \
             .withColumn("Total_Population",split(col("col1"), ";").getItem(5).cast(IntegerType())) \
             .withColumn("Number_of_Veterans",split(col("col1"), ";").getItem(6).cast(IntegerType())) \
             .withColumn("Foreign-born",split(col("col1"), ";").getItem(7).cast(IntegerType())) \
             .withColumn("Average_Household Size",split(col("col1"), ";").getItem(8).cast(FloatType())) \
             .withColumn("State_Code",split(col("col1"), ";").getItem(9)) \
             .withColumn("Race",split(col("col1"), ";").getItem(10)) \
             .withColumn("Count",split(col("col1"), ";").getItem(11).cast(IntegerType())) \
             .drop("col1")
     
        df_us_cities_demo.repartition(10)\
            .write\
            .csv(path = self._save_path + '/demo/', sep = '|', mode='overwrite', compression='gzip', header=True, timestampFormat = 'yyyy-MM-dd HH:mm:ss.SSS', quote = '"', escape = '"')



def transform_country(self):
       logging.debug("Inside transform us demographics dataset module")
       schema=StructType([StructField("value", IntegerType(), True),
                         StructField("i94cntyl", StringType(), True)
                         ])
       df_country = self._spark.read.csv(self._load_path + 'rescitycntry/I94City_Res.csv', \ 
                                       header=True, mode='PERMISSIVE',\
                                       Schema=schema)      
       df_country = df_country.withColumn('i94cntyl', regexp_replace('i94cntyl', "\'", ""))
       
       df_country = df_country.withColumnRenamed("value", "country_id") \
                              .withColumnRenamed("i94cntyl", "country")
    
       df_country.repartition(10)\
            .write\
            .csv(path = self._save_path + '/rescitycntry/', sep = '|', mode='overwrite', compression='gzip', header=True, timestampFormat = 'yyyy-MM-dd HH:mm:ss.SSS', quote = '"', escape = '"')

def transform_state(self):
       logging.debug("Inside transform us demographics dataset module")
       schema=StructType([StructField("value", IntegerType(), True),
                         StructField("i94addrl", StringType(), True)
                         ])
       df_Address = self._spark.read.csv(self._load_path + 'addrstate/I94ADDR_State.csv', \ 
                                       header=True, mode='PERMISSIVE',\
                                       Schema=schema)      
       newDf = df_Address.withColumn('value',regexp_replace('value',"\'","")) \
                         .withColumn('i94addrl', regexp_replace('i94addrl', "\'", ""))
       
       df_State = newDf.withColumnRenamed("value", "state_id") \
                       .withColumnRenamed("i94addrl", "state_name")
    
       df_State.repartition(10)\
            .write\
            .csv(path = self._save_path + '/addrstate/', sep = '|', mode='overwrite', compression='gzip', header=True, timestampFormat = 'yyyy-MM-dd HH:mm:ss.SSS', quote = '"', escape = '"')


def transform_port(self):
       logging.debug("Inside transform us demographics dataset module")
       schema=StructType([StructField("value", IntegerType(), True),
                         StructField("i94prtl", StringType(), True)
                         ])
       df_port = self._spark.read.csv(self._load_path + 'port/I94_Port.csv', \ 
                                       header=True, mode='PERMISSIVE',\
                                       Schema=schema)      
       df_port = df_port.withColumn('i94prtl', regexp_replace('i94prtl', "\'", ""))
       
       df_port = df_port.withColumnRenamed("value", "port_id") \
                              .withColumnRenamed("i94prtl", "port_name")
    
       df_port.repartition(10)\
            .write\
            .csv(path = self._save_path + '/port/', sep = '|', mode='overwrite', compression='gzip', header=True, timestampFormat = 'yyyy-MM-dd HH:mm:ss.SSS', quote = '"', escape = '"')
  