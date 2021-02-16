
type SendMessageToTopicRequest{
    .broker:string
    .clientId:string
    .topic:string
    .payload:void{
        .key:undefined
        .value:undefined
    }
}

type SendMessageToTopicResponse: void{
    .partition:long
    .offset:long
}

type SetConsumerRequest{
    .broker:string
    .groupId:string
    .topic:string
    .keyType:string
    .valueType:string
    .operation:string
    .autocommit:bool
    .duration?:long
}

type ConsumerInRequest{
    .payload*:void{
        .offset:long
        .key:undefined
        .value:undefined
    }    
}


interface KafkaConnectorControl{
    RequestResponse:
    sendMessageToTopic(SendMessageToTopicRequest)(SendMessageToTopicResponse)
    OneWay:
    setConsumer(SetConsumerRequest)
}






service Kafka {
  
inputPort ip {
        location:"local"
        interfaces: KafkaConnectorControl
    }

foreign java {
  class: "org.jolie.kafkaconnector.JolieKafkaService" 
  }
}
