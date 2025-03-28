mkdir -p rill_data
sudo chown -R 1001:1001 rill_data

rm *.zip
rm -rf ./dags/zips/*
zip -r spark_jobs.zip spark_jobs/
cp -r spark_jobs* ./dags/zips/
