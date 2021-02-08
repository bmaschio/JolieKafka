package org.jolie.kafkaconnector;

public enum JolieKafkaTypeEnum {

    INTEGER ("int"),
    DOUBLE ("double"),
    LONG  ("long"),
    STRING ("string"),
    BYTES ("bytes"),
    VALUE ("value");

    private String type;

    private JolieKafkaTypeEnum (String type){
        this.type = type;
    }

    public String type(){
        return type;
    }
}
