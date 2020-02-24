/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jolie.kafkaconnector;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import jolie.runtime.Value;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;


/**
 * @author maschio
 */
public class JolieConsumerCreator {

    public static Consumer<Object, Object> createConsumer(Value v, org.jolie.kafkaconnector.JolieKafkaTypeEnum keyType , org.jolie.kafkaconnector.JolieKafkaTypeEnum valueType) throws Exception {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, v.getFirstChild("broker").strValue());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, v.getFirstChild("groupId").strValue());
        switch (keyType) {
            case STRING:
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                break;
            case INTEGER:
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
                break;
            case LONG:
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
                break;
            case VALUE:
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.jolie.kafkaconnector.ValueJolieDeserializer.class.getName());
                break;
            case BYTES:
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
                break;
            case DOUBLE:
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, DoubleDeserializer.class.getName());
                break;
            default:
                break;
        }

        switch (valueType) {
            case STRING:
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                break;
            case INTEGER:
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
                break;
            case LONG:
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
                break;
            case VALUE:
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.jolie.kafkaconnector.ValueJolieDeserializer.class.getName());
                break;
            case BYTES:
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
                break;
            case DOUBLE:
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DoubleDeserializer.class.getName());
                break;
            default:
                throw (new Exception());
        }

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,v.getFirstChild("autocommit").boolValue());
        Consumer<Object, Object> consumer = new KafkaConsumer<>(props);
        TopicPartition partition = new TopicPartition(v.getFirstChild("topic").strValue(), 0);
        consumer.assign(Arrays.asList(partition));
        return consumer;
    }
}
