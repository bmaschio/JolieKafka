from "./packages/KafkaConnector"  import Kafka
from "security_utils" import SecurityUtils



service ProducerService {
    embed SecurityUtils as SecurityUtils
    embed Kafka as Kafka
    main{
    
        createSecureToken@SecurityUtils(  )( token )
        with(req){

            .broker="localhost:9092";
            .clientId="client1";
            .topic="topicOne";
            .payload.key = token;
            .payload.value.stock="FCA";
            .payload.value.price = 20.0
        }
        sendMessageToTopic@Kafka(req)()

        
        undef (req)

        createSecureToken@SecurityUtils(  )( token )
        
        with(req){

            .broker="localhost:9092";
            .clientId="client2";
            .topic="topicTwo";
            .payload.key = token;
            .payload.value.info="This is a payload";
            .payload.value.price = 20.0
        }
        sendMessageToTopic@Kafka(req)()
    
    }

}
