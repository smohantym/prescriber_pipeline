import logging
import logging.config
import pandas

logging.config.fileConfig(fname='../util/log.conf')
logger = logging.getLogger(__name__)


def getCurrentDate(spark):
    try:
        opDF = spark.sql(""" select current_date """)
    except NameError as exp:
        logger.error('Error in the method' + str(exp), exc_info=True)
        raise
    except Exception as exp:
        logger.error('Error in the method' + str(exp), exc_info=True)
    else:
        logger.info("Spark is validated")
        return opDF


def count(df, dfName):
    try:
        logger.info(f"...dfCount() started for Dataframe {dfName}...")
        dfCount = df.count()
        logger.info(f"The Dataframe count is {dfCount}.")
    except Exception as exp:
        logger.error("Error in dfCount()" + str(exp), exc_info=True)
        raise
    else:
        logger.info(f"The Dataframe validation by count dfCount is completed.")
        return dfCount


def top10record(df, dfName):
    try:
        logger.info(f"...dfTop10record() started for Dataframe {dfName}...")
        logger.info(f"...The Dataframe for top 10 records are:...")
        df_pandas = df.limit(10).toPandas()
        logger.info('\n \t' + df_pandas.to_string(index=False))
    except Exception as exp:
        logger.error("Error in dfTop10record()" + str(exp), exc_info=True)
        raise
    else:
        logger.info(f"The Dataframe validation for top 10 records is completed.")
        return df_pandas


def printSchema(df, dfName):
    try:
        logger.info(f"...dfPrintSchema() started for Dataframe {dfName}...")
        logger.info(f"...The Dataframe {dfName} Schema are:...")
        schemaFields = df.schema.fields
        for sf in schemaFields:
            logger.info(f"\t{sf}")
    except Exception as exp:
        logger.error("Error in dfPrintSchema()" + str(exp), exc_info=True)
        raise
    else:
        logger.info(f"The Dataframe Schema validation is completed.")
        return schemaFields
