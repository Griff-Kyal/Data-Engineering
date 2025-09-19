import os

#URL of NYC yellow trip data to extract
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-07.parquet"

#Postgres database information
dbusername = "postgres"
dbpassword = "secret"
dbhost = "localhost"
dbport = 5432
dbname = "postgres-db"


#Do not change
filename = os.path.basename(url)

date_part = filename.split("_")[2].replace(".parquet", "")
date_part = date_part.replace("-","_")
