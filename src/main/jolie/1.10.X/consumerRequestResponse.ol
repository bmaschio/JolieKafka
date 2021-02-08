include "/public/interfaces/KafkaConnector.iol"
include "console.iol"
include "time.iol"

interface ConsumerInterface{
  RequestResponse:
  consumerInReqRes(undefined)(undefined) throws ErrorProcess (undefined)
  OneWay:
  consumerInOneWay(undefined)
}

inputPort KafkaInput {
  location:"local"
  Interfaces: ConsumerInterface
}

init {

    with(req){
        .broker="localhost:9092";
        .groupId="group1";
        .topic="ProvaJolie1";
        .keyType = "string";
        .valueType = "value";
        .duration = 60000L
        .autocommit=true
        .operation = "consumerInOneWay"
    }
    setConsumer@Kafka(req)
}

execution { concurrent }

init{
  global.offset = 5
}

main{
[consumerInReqRes(request)(response){
       sleep@Time( 1000 )(  )
       global.offset += 10
       response.offset =  global.offset;
      // throw(Error, response )
       response.offset = 10L
}]
[consumerInOneWay(request)]{
  nullProcess
}
}