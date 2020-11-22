from pyspark.sql.types import StringType,TimestampType
from pyspark.sql.functions import udf
from time import strptime
from datetime import datetime, timedelta
from pyspark.sql import types as T


def convert_datetime(x):
    try:
        start = datetime(1960, 1, 1)
        return start + timedelta(days=int(x))
    except:
        return None
udf_datetime_from_sas = udf(lambda x: convert_datetime(x), T.DateType())