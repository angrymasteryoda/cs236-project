cd ../mapreduce
python3.10 main.py
hdfs dfs -put merged.csv /
mvn clean package
rm -r output/
hadoop jar target/mapreduce-1.0-SNAPSHOT.jar hdfs:///merged.csv file://`pwd`/output/