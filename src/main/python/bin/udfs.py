from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


@udf(returnType=IntegerType())
def columnSplitCount(column):
    return len(column.split(' '))
