package joliekafkaconnector;

import jolie.runtime.JavaService;
import jolie.runtime.Value;
import jolie.runtime.embedding.RequestResponse;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.ExecutionException;


public class JolieKafkaService extends JavaService {

    private  class ConsumerThread extends Thread {
        private boolean keepRun = true;

        public void kill()
        {
            keepRun = false;
            this.interrupt();
        }

        @Override
        public void run()
        {

        }



    }
    @RequestResponse
    public Value setProducer(Value v){

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
    public Value setConsumer(Value v){

            return Value.create();
    }
}
