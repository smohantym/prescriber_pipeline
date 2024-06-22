############################################################
# Developed By: Sandeep Mohanty                            #
# Developed Date:                                          # 
# Script NAME:                                             #
# PURPOSE: Copy input vendor files from local to HDFS.     #
############################################################

# Declare a variable to hold the unix script name.
JOBNAME="hdfs_to_local.ksh"

#Declare a variable to hold the current date
date=$(date '+%Y-%m-%d_%H:%M:%S')

#Define a Log File where lpgs would be generated
LOGFILE="/home/sandeepmohantym3003/data_lab/prescriber_pipeline/src/main/python/logs/${JOBNAME}_${date}.log"

########################################################################################
### COMMENTS: all standard output and standard error will be logged in the log file. ###
########################################################################################
{  # <Start of the log file.
echo "${JOBNAME} Started: $(date)"
LOCAL_STAGING_PATH="/home/sandeepmohantym3003/data_lab/prescriber_pipeline/src/main/python/output"
LOCAL_CITY_DIR=${LOCAL_STAGING_PATH}/dimension_city
LOCAL_FACT_DIR=${LOCAL_STAGING_PATH}/fact

HDFS_STAGING_PATH=prescriber_pipeline/output
HDFS_CITY_DIR=${HDFS_STAGING_PATH}/dimension_city
HDFS_FACT_DIR=${HDFS_STAGING_PATH}/fact

### Copy from hdfs to local City  and Fact folder
hdfs dfs -get -f ${HDFS_CITY_DIR}/* ${LOCAL_CITY_DIR}/
hdfs dfs -get -f ${HDFS_FACT_DIR}/* ${LOCAL_FACT_DIR}/
echo "${JOBNAME} is Completed: $(date)"
} > ${LOGFILE} 2>&1  # <End of program and end of log.

