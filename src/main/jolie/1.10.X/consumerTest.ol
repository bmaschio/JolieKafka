from "./packages/KafkaConnector"  import Kafka
from "./packages/KafkaConnector" import ConsumerInRequest
from console import Console

interface ServiceInterface {
    RequestResponse:
      topicTwoListener(ConsumerInRequest)(void)
    OneWay:
      topicOneListener(ConsumerInRequest)
  }


service TestConsumer {


  inputPort ServiceInput {
      location:"local"  
      Interfaces: ServiceInterface
  }
embed Kafka  as Kafka
embed Console as Console
embed Kafka as Kafka1 

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
    setConsumer@Kafka1(req)
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
}
