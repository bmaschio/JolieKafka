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

    public static Consumer<Object, Object> createConsumer(Value v, JolieKafkaTypeEnum keyType , JolieKafkaTypeEnum valueType) throws Exception {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, v.getFirstChild("broker").strValue());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, v.getFirstChild("groupId").strValue());

        switch (keyType) {
            case STRING:
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                break;
            case INTEGER:
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
                break;
            case LONG:
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
                break;
            case VALUE:
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ValueJolieSerializer.class.getName());
                break;
            case BYTES:
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
                break;
            case DOUBLE:
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, DoubleDeserializer.class.getName());
                break;
            default:
                break;
        }

        switch (valueType) {
            case STRING:
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                break;
            case INTEGER:
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
                break;
            case LONG:
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
                break;
            case VALUE:
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ValueJolieSerializer.class.getName());
                break;
            case BYTES:
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
                break;
            case DOUBLE:
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DoubleDeserializer.class.getName());
                break;
            default:
                throw (new Exception());
        }

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, IKafkaConstants.OFFSET_RESET_EARLIER);
        Consumer<Object, Object> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList( v.getFirstChild("topic").strValue()));
        return consumer;
    }
}
