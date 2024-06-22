import logging
import logging.config

logging.config.fileConfig(fname='../util/log.conf')
logger = logging.getLogger(__name__)


def data_persist_postgres(spark, df, dfName, url, driver, dbTable, mode, user, password):
    try:
        logger.info(f"Data Persist into postgres using data_persist_postgres() for dataframe" + dfName)
        df.write.format("jdbc") \
            .option("url", url) \
            .option("dbtable", dbTable) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", driver) \
            .mode(mode) \
            .save()
    except Exception as exp:
        logger.error(f"Error in data_persist_postgres() for dataframe" + str(exp), exc_info=True)
        raise
    else:
        logger.info(
            f"Data Persist into postgres using data_persist_postgres() for dataframe" + dfName + "is completed.")
