/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jolie.kafkaconnector;

/**
 * @author maschio
 */


import java.util.Properties;

import jolie.runtime.Value;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.*;


public class JolieProducerCreator {

    public static Producer<Object, Object> createProducer(Value v, org.jolie.kafkaconnector.JolieKafkaTypeEnum keyType, org.jolie.kafkaconnector.JolieKafkaTypeEnum valueType) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, v.getFirstChild("broker").strValue());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, v.getFirstChild("clientId").strValue());



        switch (keyType) {
            case STRING:
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                break;
            case INTEGER:
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
                break;
            case LONG:
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
                break;
            case VALUE:
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.jolie.kafkaconnector.ValueJolieSerializer.class.getName());
                break;
            case BYTES:
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
                break;
            case DOUBLE:
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, DoubleSerializer.class.getName());
                break;
            default:
                break;
        }

        switch (valueType) {
            case STRING:
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                break;
            case INTEGER:
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
                break;
            case LONG:
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
                break;
            case VALUE:
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.jolie.kafkaconnector.ValueJolieSerializer.class.getName());
                break;
            case BYTES:
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
                break;
            case DOUBLE:
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DoubleSerializer.class.getName());
                break;
            default:
                throw (new Exception());
        }

        return new KafkaProducer<>(props);
    }

}
