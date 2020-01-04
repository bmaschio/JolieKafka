
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
}

type ConsumerInRequest{
    .payload:void{
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

interface KafkaConnectorData{
    OneWay:
    consumerIn(ConsumerInRequest)
}


outputPort Kafka {
  Interfaces: KafkaConnectorControl
}

embedded {
  Java:
    "org.jolie.kafkaconnector.JolieKafkaService" in Kafka
}