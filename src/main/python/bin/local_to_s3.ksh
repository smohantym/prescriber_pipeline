############################################################
# Developed By: Sandeep Mohanty                            #
# Developed Date:                                          # 
# Script NAME:                                             #
# PURPOSE: Copy input vendor files from local to HDFS.     #
############################################################

# Declare a variable to hold the unix script name.
JOBNAME="local_to_s3.ksh"

#Declare a variable to hold the current date
date=$(date '+%Y-%m-%d_%H:%M:%S')
bucket_subdir_name=$(date '+%Y-%m-%d_%H:%M:%S')

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

for file in ${LOCAL_CITY_DIR}/*.*
do
  aws s3 --profile gcpuser cp ${file} "s3://prescriber-pipeline/dimension_city/$bucket_subdir_name/"
  echo "city file $file is pushed to s3"
done

for file in ${LOCAL_FACT_DIR}/*.*
do
  aws s3 --profile gcpuser cp ${file} "s3://prescriber-pipeline/prescriber/$bucket_subdir_name/"
  echo "prescriber file $file is pushed to s3"
done

echo "${JOBNAME} is Completed: $(date)"
} > ${LOGFILE} 2>&1  # <End of program and end of log.
