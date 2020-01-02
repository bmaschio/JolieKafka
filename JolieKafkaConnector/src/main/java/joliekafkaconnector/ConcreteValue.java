package joliekafkaconnector;

import jolie.runtime.Value;

public class ConcreteValue {
    private Value value;
    public ConcreteValue(Value value){
        this.value = value;
    }
    public Value getValue(){
        return value;
    }
}
