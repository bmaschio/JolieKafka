include "/public/interfaces/KafkaConnector.iol"
include "console.iol"

inputPort KafkaInput {
  location:"local"  
  Interfaces: KafkaConnectorData
}

init {
  
    with(req){
        .broker="localhost:9092";
        .groupId="group1";
        .topic="ProvaJolie1";
        .keyType = "string";
        .valueType = "value";
        .duration = 10L
    }
    setConsumer@Kafka(req)
}   

execution { concurrent }

main{
[consumerIn(request)]{
       println@Console( #request.payload )(  )
}
}
