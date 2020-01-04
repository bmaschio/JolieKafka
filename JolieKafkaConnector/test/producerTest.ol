include "/public/interfaces/KafkaConnector.iol"

main{
    with(req){
        .broker="localhost:9092";
        .clientId="client1";
        .topic="ProvaJolie1";
        .payload.key = 2;
        .payload.value.node1 = "Prova che bello";
        .payload.value.node2 = 21.0
    };
    sendMessageToTopic@Kafka(req)()

}
