# JolieKafka
This Project contain the connector to push and retrive Jolie messages to and from a Kafka Topic

## Starting Kafka
In both jolie version supported it exists a [docker-compose.yml](https://github.com/jolie-storm/JolieKafka/tree/master/src/main/jolie/1.10.X) that will start a local instance of kafka and zookeeper.

## Operations 

|Operation name | Functionality | Note |
|---------------|---------------|------|
| sendMessageToTopic| send a message for a specific topic || 
| setConsumer| set the operation that will recive the messages from kafka they can be OneWay or RequestResponse || 



