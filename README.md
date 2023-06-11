# cs236-project
The map reduce project for CS236 (DBs) @UCR 

**Requirements**  
- python >= 3.10
- java >= 1.11
- hadoop >= 3.3.5
- maven >= 3.6.3

**Running**  

To run the project go to the `scripts` directory and run the `run.sh`

It will merge the code with python3.10 then will put the merged csv into the hdfs. From there it will run mvn clean package and then run hadoop with the produced jar in `mapreduce/target/mapreduce-1.0-SNAPSHOT.jar`

**File Structure**  
```
│   README.md  
├───data  
│       customer-reservations.csv  
│       hotel-booking.csv  
├───mapreduce  
│   │   pom.xml   
│   │   mapreduce.sh  
│   ├───src  
│   │   ├───main  
│   │   │   └───java  
│   │   │       └───com  
│   │   │           └───mapreduce  
│   │   │                   App.java
│   │   └───test  
│   │       └───java  
│   │           └───com  
│   │               └───mapreduce  
│   │                       AppTest.java  
│   └───target  
│        └───mapreduce-1.0-SNAPSHOT.jar  
└───scripts  
    |   run.sh  
```

