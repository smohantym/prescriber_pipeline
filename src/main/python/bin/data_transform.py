from pyspark.sql.functions import upper, countDistinct, sum, split, dense_rank, col
from pyspark.sql.window import Window
import logging
import logging.config
from udfs import columnSplitCount

logging.config.fileConfig(fname='../util/log.conf')
logger = logging.getLogger(__name__)


def cityReport(dfCity, dfFact):
    try:
        logger.info("Transform cityreport() started.")
        dfCity = dfCity.dropna(subset="zips")
        dfCityZipCount = dfCity.withColumn('zip_counts', columnSplitCount(dfCity.zips))
        dfFactAgg = dfFact.groupBy(dfFact.presc_state, dfFact.presc_city).agg(countDistinct("presc_id").alias("presc_counts"), sum("trx_cnt").alias("trx_counts"))
        dfCityFactJoin = dfCityZipCount.join(dfFactAgg, (dfCityZipCount.state_id == dfFactAgg.presc_state) & (dfCityZipCount.city == dfFactAgg.presc_city),'inner')
        dfCityFactFinal = dfCityFactJoin.select("city", "state_name", "county_name", "population", "zip_counts", "trx_counts", "presc_counts")
    except Exception as exp:
        logger.error("Error in cityReport()" + str(exp), exc_info=True)
        raise
    else:
        logger.info("Transform cityreport() completed.")
    return dfCityFactFinal

def topStatePrescribers(df):
    try:
        logger.info("Transform topStatePrescribers() started.")
        statePartition = Window.partitionBy("presc_state").orderBy(col("trx_cnt").desc())
        dfPrescriberFinal = df.select("presc_id", "presc_fullname", "presc_state", "country_name", "years_of_exp", "trx_cnt","total_day_supply", "total_drug_cost") \
                                            .filter((df.years_of_exp >=20) & (df.years_of_exp <=50)) \
                                            .withColumn("dense_rank", dense_rank().over(statePartition)) \
                                            .filter(col("dense_rank") <= 5) \
                                            .select("presc_id", "presc_fullname", "presc_state", "country_name", "years_of_exp", "trx_cnt", "total_day_supply", "total_drug_cost")
    except Exception as exp:
        logger.error("Error in topStatePrescribers()" + str(exp), exc_info=True)
    else:
        logger.info("Transform topStatePrescribers() completed.")
    return dfPrescriberFinal
