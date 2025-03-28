rm *.zip
rm -rf ./dags/zips/*
zip -r spark_jobs.zip spark_jobs/
cp -r spark_jobs* ./dags/zips/