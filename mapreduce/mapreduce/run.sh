rm -r output/
mvn package
hadoop jar target/mapreduce-1.0-SNAPSHOT.jar hdfs:///merged.csv file://`pwd`/output/