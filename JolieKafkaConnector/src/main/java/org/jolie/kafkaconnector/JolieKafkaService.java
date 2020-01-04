/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jolie.kafkaconnector;


import jolie.net.CommMessage;
import jolie.runtime.AndJarDeps;
import jolie.runtime.JavaService;
import jolie.runtime.Value;
import jolie.runtime.embedding.RequestResponse;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.consumer.Consumer;

import java.util.concurrent.ExecutionException;


@AndJarDeps({"jolie-js.jar", "kafka_2.13-2.4.0.jar", "junit-4.12.jar", "slf4j-api-1.7.30.jar", "jackson-databind-2.10.0.jar", "jackson-core-2.10.0.jar", "jackson-annotations-2.10.0.jar", "json-simple-1.1.1.jar"})
public class JolieKafkaService extends JavaService {

    private class ConsumerThread extends Thread {
        private boolean keepRun = true;
        private Consumer<Object, Object> consumer;

        public void kill() {
            keepRun = false;
            consumer.close();
            this.interrupt();
        }

        @Override
        public void run() {
            while (keepRun) {
                ConsumerRecords<Object, Object> consumerRecords = consumer.poll(1);
                consumerRecords.forEach(record -> {
                    Value recordValue = Value.create();
                    recordValue.getFirstChild("payload").getFirstChild("key").setValue(record.key());
                    if (record.value() instanceof Value) {

                        recordValue.getFirstChild("payload").getFirstChild("value").deepCopy((Value) record.value());
                    } else {
                        recordValue.getFirstChild("payload").getFirstChild("value").setValue(record.value());
                    }
                    sendMessage(CommMessage.createRequest("consumerIn", "/", recordValue));
                });

            }
        }


        public void setValue(Value v) throws Exception {
            org.jolie.kafkaconnector.JolieKafkaTypeEnum keyType = null;
            org.jolie.kafkaconnector.JolieKafkaTypeEnum valueType = null;
            if (v.getFirstChild("keyType").strValue().equals("long")) {
                keyType = org.jolie.kafkaconnector.JolieKafkaTypeEnum.LONG;
            } else if (v.getFirstChild("keyType").strValue().equals("int")) {
                keyType = org.jolie.kafkaconnector.JolieKafkaTypeEnum.INTEGER;
            } else if (v.getFirstChild("keyType").strValue().equals("string")) {
                keyType = org.jolie.kafkaconnector.JolieKafkaTypeEnum.STRING;
            }

            if (v.getFirstChild("valueType").strValue().equals("long")) {
                valueType = org.jolie.kafkaconnector.JolieKafkaTypeEnum.LONG;
            } else if (v.getFirstChild("valueType").strValue().equals("int")) {
                valueType = org.jolie.kafkaconnector.JolieKafkaTypeEnum.INTEGER;
            } else if (v.getFirstChild("valueType").strValue().equals("string")) {
                valueType = org.jolie.kafkaconnector.JolieKafkaTypeEnum.STRING;
            } else if (v.getFirstChild("valueType").strValue().equals("double")) {
                valueType = org.jolie.kafkaconnector.JolieKafkaTypeEnum.DOUBLE;
            } else if (v.getFirstChild("valueType").strValue().equals("value")) {
                valueType = org.jolie.kafkaconnector.JolieKafkaTypeEnum.VALUE;
            }
            consumer = org.jolie.kafkaconnector.JolieConsumerCreator.createConsumer(v, keyType, valueType);
        }

    }

    private ConsumerThread consumerThread;

    @RequestResponse
    public Value sendMessageToTopic(Value v) {
        Value returnValue = Value.create();
        for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
            Object key;
            Object value;
            org.jolie.kafkaconnector.JolieKafkaTypeEnum keyType = null;
            org.jolie.kafkaconnector.JolieKafkaTypeEnum valueType = null;
            if (v.getFirstChild("payload").getFirstChild("key").isLong()) {
                key = new Long(v.getFirstChild("payload").getFirstChild("key").longValue());
                keyType = org.jolie.kafkaconnector.JolieKafkaTypeEnum.LONG;
            } else if (v.getFirstChild("payload").getFirstChild("key").isInt()) {
                key = new Integer(v.getFirstChild("payload").getFirstChild("key").intValue());
                keyType = org.jolie.kafkaconnector.JolieKafkaTypeEnum.INTEGER;
            } else if (v.getFirstChild("payload").getFirstChild("key").isString()) {
                key = new String(v.getFirstChild("payload").getFirstChild("key").strValue());
                keyType = org.jolie.kafkaconnector.JolieKafkaTypeEnum.STRING;
            } else {
                key = null;
            }

            if (v.getFirstChild("payload").getFirstChild("value").isLong()) {
                value = new Long(v.getFirstChild("payload").getFirstChild("value").longValue());
                valueType = org.jolie.kafkaconnector.JolieKafkaTypeEnum.LONG;
            } else if (v.getFirstChild("payload").getFirstChild("value").isInt()) {
                value = new Integer(v.getFirstChild("payload").getFirstChild("value").intValue());
                valueType = org.jolie.kafkaconnector.JolieKafkaTypeEnum.INTEGER;
            } else if (v.getFirstChild("payload").getFirstChild("value").isString()) {
                value = new String(v.getFirstChild("payload").getFirstChild("value").strValue());
                valueType = org.jolie.kafkaconnector.JolieKafkaTypeEnum.STRING;
            } else if (v.getFirstChild("payload").getFirstChild("value").hasChildren()) {
                value = v.getFirstChild("payload").getFirstChild("value");
                valueType = org.jolie.kafkaconnector.JolieKafkaTypeEnum.VALUE;
            } else {
                value = null;
            }
            Producer<Object, Object> producer = null;
            try {
                producer = org.jolie.kafkaconnector.JolieProducerCreator.createProducer(v, keyType, valueType);
            } catch (Exception e) {
                e.printStackTrace();
            }
            ProducerRecord<Object, Object> record = new ProducerRecord<>(v.getFirstChild("topic").strValue(), key, value);

            try {
                RecordMetadata metadata = producer.send(record).get();
                returnValue.getFirstChild("partition").setValue(metadata.partition());
                returnValue.getFirstChild("offset").setValue(metadata.offset());
            } catch (ExecutionException e) {
                System.out.println("Error in sending record");
                System.out.println(e);
            } catch (InterruptedException e) {
                System.out.println("Error in sending record");
                System.out.println(e);
            }
        }
        return returnValue;
    }

    @RequestResponse
    public void setConsumer(Value v) throws Exception {
        consumerThread = new ConsumerThread();
        consumerThread.setValue(v);
        consumerThread.start();

    }

    @Override
    protected void finalize()
            throws Throwable {
        try {
            consumerThread.kill();
        } finally {
            super.finalize();
        }
    }
}
