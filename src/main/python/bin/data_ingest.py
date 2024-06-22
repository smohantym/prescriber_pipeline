import logging
import logging.config

logging.config.fileConfig(fname='../util/log.conf')
logger = logging.getLogger(__name__)


def loadFiles(spark, fileDir, fileFormat, header, inferSchema):
    try:
        logger.info("...loadFiles() Started...")
        if fileFormat == 'parquet':
            df = spark. \
                read. \
                format(fileFormat). \
                load(fileDir)
        elif fileFormat == 'csv':
            df = spark. \
                read. \
                format(fileFormat). \
                options(header=header). \
                options(inferSchema=inferSchema). \
                load(fileDir)
    except Exception as exp:
        logger.error("Error in loadFile()." + str(exp), exc_info=True)
        raise
    else:
        logger.info(f"Input file {fileDir} loaded to Data Frame.")
        return df
