package joliekafkaconnector;

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



@AndJarDeps( { "jolie-js.jar", "kafka_2.13-2.4.0.jar"  } )
public class JolieKafkaService extends JavaService {

    private  class ConsumerThread extends Thread {
        private boolean keepRun = true;
        private Consumer<Object, Object> consumer;


        public void kill()
        {
            keepRun = false;
            consumer.close();
            this.interrupt();
        }

        @Override
        public void run()
        {
              while (keepRun){
                  ConsumerRecords<Object, Object> consumerRecords = consumer.poll(1);
                  consumerRecords.forEach( record -> {
                      Value recordValue = Value.create();
                      recordValue.getFirstChild("payload").getFirstChild("key").setValue(record.key());
                      recordValue.getFirstChild("payload").getFirstChild("value").setValue(record.value());
                      sendMessage( CommMessage.createRequest( "consumerIn", "/", recordValue) );
                  });

              }
        }



        public void setValue (Value v) throws Exception {
            JolieKafkaTypeEnum keyType = null ;
            JolieKafkaTypeEnum valueType = null ;
            if (v.getFirstChild("keyType").strValue().equals("long")){
                keyType = JolieKafkaTypeEnum.LONG;
            } else if (v.getFirstChild("keyType").strValue().equals("int")){
                keyType = JolieKafkaTypeEnum.INTEGER;
            } else if (v.getFirstChild("keyType").strValue().equals("string")){
                keyType = JolieKafkaTypeEnum.STRING;
            }

            if (v.getFirstChild("valueType").strValue().equals("long")){
                valueType = JolieKafkaTypeEnum.LONG;
            } else if (v.getFirstChild("valueType").strValue().equals("int")){
                valueType = JolieKafkaTypeEnum.INTEGER;
            } else if (v.getFirstChild("valueType").strValue().equals("string")){
                valueType = JolieKafkaTypeEnum.STRING;
            }else if (v.getFirstChild("valueType").strValue().equals("double")){
                valueType = JolieKafkaTypeEnum.DOUBLE;
            } else if (v.getFirstChild("valueType").strValue().equals("value")){
                valueType = JolieKafkaTypeEnum.VALUE;
            }
            consumer = JolieConsumerCreator.createConsumer(v , keyType ,valueType);
        }

    }

    private  ConsumerThread consumerThread;

    @RequestResponse
    public Value sendMessageToTopic(Value v){

        for( int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++ ) {
            Object key;
            Object value;
            JolieKafkaTypeEnum keyType = null ;
            JolieKafkaTypeEnum valueType = null ;
            if (v.getFirstChild("payload").getFirstChild("key").isLong()){
                key = new Long(v.getFirstChild("payload").getFirstChild("key").longValue());
                keyType = JolieKafkaTypeEnum.LONG;
            } else if (v.getFirstChild("payload").getFirstChild("key").isInt()){
                key = new Integer(v.getFirstChild("payload").getFirstChild("key").intValue());
                keyType = JolieKafkaTypeEnum.INTEGER;
            } else if (v.getFirstChild("payload").getFirstChild("key").isString()){
                key = new String (v.getFirstChild("payload").getFirstChild("key").strValue());
                keyType = JolieKafkaTypeEnum.STRING;
            } else {
                key = null;
            }

            if (v.getFirstChild("payload").getFirstChild("value").isLong()){
                value = new Long(v.getFirstChild("payload").getFirstChild("value").longValue());
                valueType =JolieKafkaTypeEnum.LONG;
            } else if (v.getFirstChild("payload").getFirstChild("value").isInt()){
                value = new Integer(v.getFirstChild("payload").getFirstChild("value").intValue());
                valueType =JolieKafkaTypeEnum.INTEGER;
            } else if (v.getFirstChild("payload").getFirstChild("value").isString()){
                value = new String (v.getFirstChild("payload").getFirstChild("value").strValue());
                valueType =JolieKafkaTypeEnum.STRING;
            } else if (v.getFirstChild("payload").getFirstChild("value").hasChildren()){
                value = v.getFirstChild("payload").getFirstChild("value");
                valueType =JolieKafkaTypeEnum.VALUE;
            } else {
                value = null;
            }
            Producer<Object, Object> producer = null;
            try {
                producer = JolieProducerCreator.createProducer(v,keyType,valueType);
            } catch (Exception e) {
                e.printStackTrace();
            }
            ProducerRecord<Object, Object> record =new ProducerRecord<>(v.getFirstChild("topic").strValue(),key,value);

            try {
                RecordMetadata metadata = producer.send( record ).get();
                System.out.println( "Record sent with key " + index + " to partition " + metadata.partition() + " with offset " + metadata.offset() );
            } catch( ExecutionException e ) {
                System.out.println( "Error in sending record" );
                System.out.println( e );
            } catch( InterruptedException e ) {
                System.out.println( "Error in sending record" );
                System.out.println( e );
            }
        }
        return Value.create();
    }
    @RequestResponse
    public void setConsumer(Value v) throws Exception {
       consumerThread = new ConsumerThread();
       consumerThread.setValue(v);
       consumerThread.start();

    }

    @Override
    protected void finalize()
            throws Throwable
    {
        try {
            consumerThread.kill();
        } finally {
            super.finalize();
        }
    }
}
