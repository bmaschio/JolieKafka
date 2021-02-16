include "/public/interfaces/KafkaConnector.iol"
include "console.iol"
include "time.iol"

interface ServiceInterface {
    RequestResponse:
      topicTwoListener(ConsumerInRequest)(void)
    OneWay:
      topicOneListener(ConsumerInRequest)
  }

inputPort KafkaInput {
  location:"local"
  Interfaces: ServiceInterface
}

init {
  
    with(req){
        .broker="localhost:9092";
        .groupId="group1";
        .topic="topicOne";
        .keyType = "string";
        .valueType = "value";
        .duration = 10L;
        .operation = "topicOneListener";
        .autocommit = true
    }
    setConsumer@Kafka(req)

        with(req){
        .broker="localhost:9092";
        .groupId="group1";
        .topic="topicTwo";
        .keyType = "string";
        .valueType = "value";
        .duration = 10L;
        .operation = "topicTwoListener";
        .autocommit = false
    }
    setConsumer@Kafka(req)
}   

execution { concurrent }

main{
[topicOneListener(request)]{
       println@Console( "topicOneListener" )(  )
       }
[topicTwoListener(request)(){
       println@Console( "topicTwoListener" )(  )
       }]

}
