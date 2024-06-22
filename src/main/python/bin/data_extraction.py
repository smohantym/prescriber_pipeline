import logging
import logging.config

logging.config.fileConfig(fname='../util/log.conf')
logger = logging.getLogger(__name__)


def extractFiles(df, format, filePath, split_no, headerReq, compressionType):
    try:
        logger.info(f"extractFiles() started.")
        df.coalesce(split_no) \
                .write \
                .format(format) \
                .save(filePath, header=headerReq, compression=compressionType)
    except Exception as exp:
        logger.error("Error in extractFiles()" + str(exp), exc_info=True)
        raise
    else:
        logger.info("Extraction extractFiles() Completed.")

