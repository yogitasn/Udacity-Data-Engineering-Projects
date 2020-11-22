from pyspark.sql import SparkSession
from immigration_transform import ImmigrationDatasetTransform
from s3_module import ImmigrationS3Module
from pathlib import Path
import logging
import logging.config
import time



# Setting configurations. Look config.cfg for more details

config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/config.cfg"))



# Setting up logger, Logger properties are defined in logging.ini file
logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")
logger = logging.getLogger(__name__)







def create_sparksession():

    """

    Initialize a spark session

    """

    return SparkSession.builder.master('yarn').appName("goodreads") \
           .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11") \
           .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.2") \
           .enableHiveSupport().getOrCreate()

def main():

    """
    This method performs below tasks:
    1: Check for data in Landing Zone, if new files are present move them to Working Zone
    2: Transform data present in working zone and save the transformed data to Processed Zone
    3: Run Data Warehouse functionality by setting up Staging tables, then loading staging tables, performing upsert operations on warehouse.
    """

    logging.debug("\n\nSetting up Spark Session...")
    spark = create_sparksession()
    immgrt = ImmigrationDatasetTransform(spark)


    # Modules in the project

    modules = {
        "immigration.pq": immgrt.transform_immigration_dataset,
        "airport.csv" : immgrt.transform_airport_dataset,
        "us-cities-demographics.csv" : immgrt.transform_us_demo_dataset,
    }



    logging.debug("\n\nCopying data from s3 landing zone to ...")
    gds3 = GoodReadsS3Module()
    gds3.s3_move_data(source_bucket= config.get('BUCKET','LANDING_ZONE'), target_bucket= config.get('BUCKET', 'WORKING_ZONE'))
    files_in_working_zone = gds3.get_files(config.get('BUCKET', 'WORKING_ZONE'))

    # Cleanup processed zone if files available in working zone
    if len([set(modules.keys()) & set(files_in_working_zone)]) > 0:
        logging.info("Cleaning up processed zone.")
        gds3.clean_bucket(config.get('BUCKET', 'PROCESSED_ZONE'))



    for file in files_in_working_zone:
        if file in modules.keys():
            modules[file]()

# Entry point for the pipeline
if __name__ == "__main__":
    main()