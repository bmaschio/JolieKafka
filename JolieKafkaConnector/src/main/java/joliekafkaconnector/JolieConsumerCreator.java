/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package joliekafkaconnector;

import java.util.Collections;
import java.util.Properties;

import jolie.runtime.Value;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.*;


/**
 * @author maschio
 */
public class JolieConsumerCreator {

    public static Consumer<Object, Object> createConsumer(Value v) throws Exception {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, v.getFirstChild("broker").strValue());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, v.getFirstChild("groupId").strValue());

        if (JolieKafkaTypeEnum.BYTES.type().equals(v.getFirstChild("keyType").strValue())) {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        } else if (JolieKafkaTypeEnum.INTEGER.type().equals(v.getFirstChild("keyType").strValue())) {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        } else if (JolieKafkaTypeEnum.DOUBLE.type().equals(v.getFirstChild("keyType").strValue())) {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, DoubleDeserializer.class.getName());
        } else if (JolieKafkaTypeEnum.STRING.type().equals(v.getFirstChild("keyType").strValue())) {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        } else if (JolieKafkaTypeEnum.LONG.type().equals(v.getFirstChild("keyType").strValue())) {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        } else if (JolieKafkaTypeEnum.VALUE.type().equals(v.getFirstChild("keyType").strValue())) {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ValueJolieDeserializer.class.getName());
        } else {
            throw (new Exception());
        }

        if (JolieKafkaTypeEnum.BYTES.type().equals(v.getFirstChild("valueType").strValue())) {
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        } else if (JolieKafkaTypeEnum.INTEGER.type().equals(v.getFirstChild("valueType").strValue())) {
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        } else if (JolieKafkaTypeEnum.DOUBLE.type().equals(v.getFirstChild("valueType").strValue())) {
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DoubleDeserializer.class.getName());
        } else if (JolieKafkaTypeEnum.STRING.type().equals(v.getFirstChild("valueType").strValue())) {
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        } else if (JolieKafkaTypeEnum.LONG.type().equals(v.getFirstChild("valueType").strValue())) {
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        } else if (JolieKafkaTypeEnum.VALUE.type().equals(v.getFirstChild("valueType").strValue())) {
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ValueJolieDeserializer.class.getName());
        } else {
            throw (new Exception());
        }

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, IKafkaConstants.OFFSET_RESET_EARLIER);
        Consumer<Object, Object> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(IKafkaConstants.TOPIC_NAME));
        return consumer;
    }
}
