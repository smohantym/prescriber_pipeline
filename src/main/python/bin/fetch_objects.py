from pyspark.sql import SparkSession
import logging
import logging.config

logging.config.fileConfig(fname='../util/log.conf')
logger = logging.getLogger(__name__)


def sparkObject(envn, appName):
    try:
        logger.info(f"sparkObject() started. The Environment: {envn} is being used")
        if envn == 'TEST':
            master = 'local'
        else:
            master = 'yarn'
        spark = SparkSession \
            .builder \
            .master(master) \
            .appName(appName) \
            .getOrCreate()
    except NameError as exp:
        logger.error('Error in the method' + str(exp), exc_info=True)
        raise
    except Exception as exp:
        logger.error('Error in the method' + str(exp), exc_info=True)
    else:
        logging.info("Spark Object Created")
    return spark
