import sys
import os
import logging
import logging.config
from subprocess import Popen, PIPE
import get_variables as getVars
from fetch_objects import sparkObject
from validations import getCurrentDate, count, top10record, printSchema
from data_ingest import loadFiles
from data_preprocessing import selectData, performDataCleaning
from data_transform import cityReport, topStatePrescribers
from data_extraction import extractFiles
from data_persist import data_persist_postgres

logging.config.fileConfig(fname='../util/log.conf')


def main():
    try:
        logging.info("...Pipeline Started...")
        spark = sparkObject(getVars.envn, getVars.appName)

        fileDir="prescriber_pipeline/staging/dimension_city"
        proc = Popen(['hdfs', 'dfs', '-ls', '-C', fileDir], stdout=PIPE, stderr=PIPE)
        (out, err) = proc.communicate()
        if 'parquet' in out.decode():
            fileFormat = 'parquet'
            header = 'NA'
            inferSchema = 'NA'
        elif 'csv' in out.decode():
            fileFormat = 'csv'
            header = getVars.header
            inferSchema = getVars.inferSchema
        dfCity = loadFiles(spark, fileDir, fileFormat, header, inferSchema)

        #count(dfCity, 'df_city')
        #top10record(dfCity, 'df_city')

        fileDir="prescriber_pipeline/staging/fact"
        proc = Popen(['hdfs', 'dfs', '-ls', '-C', fileDir], stdout=PIPE, stderr=PIPE)
        (out, err) = proc.communicate()
        if 'parquet' in out.decode():
            fileFormat = 'parquet'
            header = 'NA'
            inferSchema = 'NA'
        elif 'csv' in out.decode():
            fileFormat = 'csv'
            header = getVars.header
            inferSchema = getVars.inferSchema
        dfFact = loadFiles(spark, fileDir, fileFormat, header, inferSchema)

        #count(dfFact, 'df_fact')
        #top10record(dfFact, 'df_fact')
        #printSchema(dfFact, 'dfCleanFact')

        dfSelectCity = selectData(dfCity)
        #top10record(dfSelectCity, 'dfSelectCity')

        dfCleanFact = performDataCleaning(dfFact)
        #top10record(dfCleanFact, 'dfCleanFact')
        #printSchema(dfCleanFact, 'dfCleanFact')

        dfCityFact = cityReport(dfSelectCity, dfCleanFact)
        #top10record(dfCityFact, 'dfCityFact')

        dfTopStatePrescriber = topStatePrescribers(dfCleanFact)
        top10record(dfTopStatePrescriber, 'dfTopStatePrescriber')
        dfTopStatePrescriber.show()

        cityPath = getVars.outputCity
        extractFiles(dfCityFact, 'json', cityPath, 1, False, 'bzip2')

        factPath = getVars.outputFact
        extractFiles(dfTopStatePrescriber, 'orc', factPath, 2, False, 'snappy')

        data_persist_postgres(spark, dfCityFact, 'dfCityFact', "jdbc:postgresql://localhost:6432/prespipeline",
                              "org.postgresql.Driver", "city_fact", "append", getVars.user, getVars.password)

        data_persist_postgres(spark, dfTopStatePrescriber, 'dfTopStatePrescriber', "jdbc:postgresql://localhost:6432/prespipeline",
                              "org.postgresql.Driver", "top_state_prescriber", "append", getVars.user, getVars.password)
    except Exception as exp:
        logging.error("Error Occurred " + str(exp), exc_info=True)
        sys.exit(1)
    else:
        logging.info("...Pipeline Completed...")


if __name__ == "__main__":
    logging.info("...Execution Started...")
    main()
    logging.info("...Execution Completed...")
