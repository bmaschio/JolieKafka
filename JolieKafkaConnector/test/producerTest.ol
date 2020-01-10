include "/public/interfaces/KafkaConnector.iol"
include "security_utils.iol"


main{
   while (true){
    createSecureToken@SecurityUtils(  )( token )
    with(req){

        .broker="localhost:9092";
        .clientId="client1";
        .topic="ProvaJolie1";
        .payload.key = token;
        .payload.value.stock="FCA";
        .payload.value.price = 20.0
    }
    sendMessageToTopic@Kafka(req)()
   }
 

}
