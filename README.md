"# KAFKA_CHALLENGE" 

https://github.com/tamediadigital/hiring-challenges/tree/master/data-engineer-challenge


1. install kafka 

tar -xzf kafka_2.11-0.10.0.0.tgz
cd kafka_2.11-0.10.0.0 
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties

2. create a topic

bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic streams-file-input

3. use the kafka producer from kafka itself to send our test data to your topic

cat file-input.txt | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic streams-file-input

4. create a small app that reads this data from kafka and prints it to stdout

bin/kafka-run-class.sh MyCAT streams-file-input

5. find a suitable data structure for counting and implement a simple counting mechanism, output the results to stdout 

##### advanced solution

6. benchmark

7. Output to a new Kafka Topic instead of stdout

8. try to measure performance and optimize

9. write about how you could scale

10. only now think about the edge cases, options and other things


