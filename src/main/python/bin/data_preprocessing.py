import logging
import logging.config
from pyspark.sql.functions import upper, lit, col, regexp_extract, concat_ws, count, when, isnan, avg, round, coalesce
from pyspark.sql.window import Window

logging.config.fileConfig(fname='../util/log.conf')
logger = logging.getLogger(__name__)


def selectData(df):
    try:
        logger.info("selectData() Started.")
        dfSelectCity = df.select(upper(df.city).alias("city"),
                                 df.state_id,
                                 upper(df.state_name).alias("state_name"),
                                 upper(df.county_name).alias("county_name"),
                                 df.population,
                                 df.zips)
    except Exception as exp:
        logger.error("Error in selectData()" + str(exp), exc_info=True)
        raise
    else:
        logger.info("selectData() Completed.")
        return dfSelectCity


def performDataCleaning(df):
    try:
        logger.info("performDataCleaning() Started.")
        logger.info("performDataCleaning() data selection Started.")
        dfFact = df.select(df.npi.alias("presc_id"),
                           df.nppes_provider_last_org_name.alias("presc_lname"),
                           df.nppes_provider_first_name.alias("presc_fname"),
                           df.nppes_provider_city.alias("presc_city"),
                           df.nppes_provider_state.alias("presc_state"),
                           df.specialty_description.alias("presc_spclt"),
                           df.year_exp.alias("years_of_exp"),
                           df.drug_name,
                           df.total_claim_count.alias("trx_cnt"),
                           df.total_day_supply,
                           df.total_drug_cost)

        logger.info("performDataCleaning() adding country name.")
        dfFact = dfFact.withColumn("country_name", lit("USA"))

        logger.info("performDataCleaning() refactoring year_of_exp.")
        pattern = '\d+'
        idx = 0
        dfFact = dfFact.withColumn("years_of_exp", regexp_extract(col("years_of_exp"), pattern, idx))
        dfFact = dfFact.withColumn("years_of_exp", col("years_of_exp").cast("int"))

        logger.info("performDataCleaning() concat fname & lname.")
        dfFact = dfFact.withColumn("presc_fullname", concat_ws(" ", "presc_fname", "presc_lname"))
        dfFact = dfFact.drop("presc_fname", "presc_lname")

        logger.info("performDataCleaning() cleaning None or NaN values.")
        print(f"Matrics of Non & NaN values:")
        dfFact.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in dfFact.columns]).show()
        dfFact = dfFact.dropna(subset="presc_id")
        dfFact = dfFact.dropna(subset="drug_name")

        logger.info("performDataCleaning() imputing trx_cnt where it is null on avg of trx_cnt of that prescriber.")
        print(f"Matrics of trx_cnt with Null values for the prescriber:")
        dfFact.filter(dfFact.trx_cnt.isNull()).select('presc_id', 'trx_cnt').show()
        spec = Window.partitionBy("presc_id")
        dfFact = dfFact.withColumn("trx_cnt", coalesce("trx_cnt", round(avg("trx_cnt").over(spec))))

        print(f"Matrics of Non & NaN values post cleaning:")
        dfFact.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in dfFact.columns]).show()
        print(f"Matrics of trx_cnt with Null values for the prescriber post imputing:")
        dfFact.filter(dfFact.trx_cnt.isNull()).select('presc_id', 'trx_cnt').show()

    except Exception as exp:
        logger.error("Error in performDataCleaning()" + str(exp), exc_info=True)
        raise
    else:
        logger.info("performDataCleaning() Completed.")
        return dfFact
