/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jolie.kafkaconnector;


import com.sun.crypto.provider.PBEWithMD5AndDESCipher;
import jolie.Interpreter;
import jolie.lang.Constants;
import jolie.net.CommMessage;
import jolie.runtime.*;
import jolie.runtime.embedding.RequestResponse;
import jolie.runtime.typing.OperationTypeDescription;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

@AndJarDeps({ "jolie-js.jar", "kafka_2.13-2.4.0.jar", "junit-4.12.jar", "slf4j-api-1.7.30.jar",
        "jackson-databind-2.10.0.jar", "jackson-core-2.10.0.jar", "jackson-annotations-2.10.0.jar",
        "json-simple-1.1.1.jar" })
public class JolieKafkaService extends JavaService {

    private Producer<Object, Object> producer = null;
    private String producerTopic = "";
    private HashMap<String,ConsumerThread> counsumerHashMap = new HashMap();

    private class ConsumerThread extends Thread {
        private boolean keepRun = true;
        private long durationInSecond = 1;
        private Consumer<Object, Object> consumer;
        private String operation;
        private Boolean autoCommit;
        private String topic;


        public void kill() {
            keepRun = false;
            consumer.close();

            this.interrupt();
        }

        @Override
        public void run() {
            while (keepRun) {
                ConsumerRecords<Object, Object> consumerRecords = consumer.poll(Duration.ofMillis(durationInSecond));
                Value recordValue = Value.create();
                consumerRecords.forEach(record -> {
                    Value singleValue = Value.create();
                    singleValue.getFirstChild("key").setValue(record.key());
                    if (record.value() instanceof Value) {
                        singleValue.getFirstChild("value").deepCopy((Value) record.value());
                    } else {
                        singleValue.getFirstChild("value").setValue(record.value());
                    }
                    recordValue.getChildren("payload").add(singleValue);

                });
                if (recordValue.hasChildren("payload")) {
                    CommMessage request = CommMessage.createRequest(operation, "/", recordValue);
                    try {
                        CommMessage response = sendMessage(request).recvResponseFor(request).get();

                        if (autoCommit == false){
                            HashMap <TopicPartition, OffsetAndMetadata>  commitMap= new HashMap<>();
                            response.value().getFirstChild("offset").longValue();
                            commitMap.put( new TopicPartition(topic,0) , new OffsetAndMetadata( response.value().getFirstChild("offset").longValue()));
                            consumer.commitSync(commitMap);
                        }
                    }
                     catch (InterruptedException | IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    catch (ExecutionException e){
                        e.printStackTrace();
                    }
                }

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

            autoCommit = v.getFirstChild("autocommit").boolValue();
            operation =  v.getFirstChild("operation").strValue();
            topic = v.getFirstChild("topic").strValue();
            OperationTypeDescription operationType = Interpreter.getInstance().commCore().localListener().inputPort().getOperationTypeDescription( operation, Constants.ROOT_RESOURCE_PATH );
            if ((operationType instanceof RequestResponseOperation) & (autoCommit == true)){
                throw new FaultException( "RequestResponseConfigError", new Exception("Not allowed autocommit set to true with RequestResponse callback operation" ) );
            }

            if ((operationType instanceof OneWayOperation) & (autoCommit == false)){
                throw new FaultException( "OneWayConfigError", new Exception("Not allowed autocommit set to false with Oneway callback operation" ));
            }


            consumer = org.jolie.kafkaconnector.JolieConsumerCreator.createConsumer(v, keyType, valueType);


            if (v.hasChildren("duration")){
                durationInSecond = v.getFirstChild("duration").longValue();
            }

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
            if ((producer==null) && (!producerTopic.equals(v.getFirstChild("topic").strValue()))){
                try {
                    producerTopic = v.getFirstChild("topic").strValue();
                    producer = org.jolie.kafkaconnector.JolieProducerCreator.createProducer(v, keyType, valueType);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }else {
                if (!v.getFirstChild("topic").strValue().equals(producerTopic)){
                    producer.flush();
                    producer.close();
                    try {
                        producerTopic = v.getFirstChild("topic").strValue();
                        producer = org.jolie.kafkaconnector.JolieProducerCreator.createProducer(v, keyType, valueType);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
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
    public void setConsumer(Value v) throws FaultException {
        consumerThread = new ConsumerThread();
        try {
            consumerThread.setValue(v);
            counsumerHashMap.put(v.getFirstChild("operation").strValue(), consumerThread);
        } catch (Exception e) {
            throw new FaultException( "ConsumerCreationError", e );
        }
        consumerThread.start();

    }

    @Override
    protected void finalize()
            throws Throwable {
        try {
            producer.flush();
            consumerThread.kill();
            producer.close();
        } finally {
            super.finalize();
        }
    }
}
