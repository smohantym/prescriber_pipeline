############################################################
# Developed By:                                            #
# Developed Date:                                          # 
# Script NAME:                                             #
# PURPOSE: Copy input vendor files from local to HDFS.     #
############################################################

# Declare a variable to hold the unix script name.
JOBNAME="delete_hdfs_output.ksh"

#Declare a variable to hold the current date
date=$(date '+%Y-%m-%d_%H:%M:%S')

#Define a Log File where lpgs would be generated
LOGFILE="/home/sandeepmohantym3003/data_lab/prescriber_pipeline/src/main/python/logs/delete_hdfs_output${date}.log"

########################################################################################
### COMMENTS: all standard output and standard error will be logged in the log file. ###
########################################################################################
{  # <Start of the log file.
echo "${JOBNAME} Started: $(date)"
CITY_PATH=prescriber_pipeline/output/dimension_city
hdfs dfs -test -d $CITY_PATH
status=$?
if [ $status == 0 ]
  then
  echo "The HDFS output directory $CITY_PATH is available. Deleting..."
  hdfs dfs -rm -r -f $CITY_PATH
  echo "The HDFS output directory $CITY_PATH is deleted"
fi

FACT_PATH=prescriber_pipeline/output/fact
hdfs dfs -test -d $FACT_PATH
status=$?
if [ $status == 0 ]
  then
  echo "The HDFS output directory $FACT_PATH is available. Deleting..."
  hdfs dfs -rm -r -f $FACT_PATH
  echo "The HDFS output directory $FACT_PATH is deleted"
fi

echo "${JOBNAME} Completed: $(date)"
} > ${LOGFILE} 2>&1  # <--- End of program and end of log.
