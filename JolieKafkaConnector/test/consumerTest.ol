include "/public/interfaces/KafkaConnector.iol"

inputPort KafkaInput {
  location:"local"  
  Interfaces: KafkaConnectorData
}

init {
    with(req){
        .broker="localhost:9092";
        .groupId="group1";
        .topic="ProvaJolie1";
        .keyType = "int";
        .valueType = "value"
    }
    setConsumer@Kafka(req)
}   

execution { concurrent }

main{
[consumerIn(request)]{
    nullProcess
}
}
