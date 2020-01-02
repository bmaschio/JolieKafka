package joliekafkaconnector;

import jolie.runtime.JavaService;
import jolie.runtime.Value;
import jolie.runtime.embedding.RequestResponse;
import jolie.runtime.embedding.


public class JolieKafkaService extends JavaService {

    @RequestResponse
    public Value setProducer(Value v){
        return Value.create();
    }
    @OneWay
    public Value setConsumer(Value v){

    }
}
