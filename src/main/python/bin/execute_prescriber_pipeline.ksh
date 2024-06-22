printf "deleting hdfs output path.\n"
/home/sandeepmohantym3003/data_lab/prescriber_pipeline/src/main/python/bin/delete_hdfs_output.ksh
printf "deletion completed.\n"

printf "started executing the pipeline.\n"
spark3-submit --master yarn pipeline.py
printf "Execution Completed.\n"

printf "copy files from hdfs to local.\n"
/home/sandeepmohantym3003/data_lab/prescriber_pipeline/src/main/python/bin/hdfs_to_local.ksh
printf "copying completed.\n"

printf "copy files from local to s3.\n"
/home/sandeepmohantym3003/data_lab/prescriber_pipeline/src/main/python/bin/local_to_s3.ksh
printf "copying completed.\n"
