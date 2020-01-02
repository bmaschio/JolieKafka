/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package joliekafkaconnector;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import jolie.runtime.Value;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 *
 * @author maschio
 */
public class JolieKafkaConnector
{

	/**
	 * @param args the command line arguments
	 */
	public static void main( String[] args ) throws Exception {
		while (true){
			Value v = Value.create();
			v.getNewChild("broker").setValue("localhost:9092");
			v.getNewChild("clientId").setValue("client1");
			v.getNewChild("topic").setValue("Prova1");
			v.getFirstChild("payload").getFirstChild("key").setValue(new Integer(1));
			v.getFirstChild("payload").getFirstChild("value").setValue(new String("Client"));
			runProducer(v);

		}
	}

	static void runConsumer()
	{
	/*	Consumer<Long, ConcreteValue> consumer = JolieConsumerCreator.createConsumer();
		int noMessageFound = 0;
		while( true ) {
			ConsumerRecords<Long, ConcreteValue> consumerRecords = consumer.poll(1);


			// 1000 is the time in milliseconds consumer will wait if no record is found at broker.
			if ( consumerRecords.count() == 0 ) {
				noMessageFound++;
				if ( noMessageFound > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT ) // If no message found count is reached to threshold exit loop.  
				{
					break;
				} else {
					continue;
				}
			}
			//print each record.
			consumerRecords.forEach( record -> {
				System.out.println( "Record Key " + record.offset() );
				System.out.println( "Record value " + record.value().getValue().getFirstChild( "dato").strValue() );
			});
		/*	consumerRecords.forEach( record -> {
				System.out.println( "Record Key " + record.key() );
				System.out.println( "Record value " + record.value().getValue().getFirstChild( "dato").strValue() );
				System.out.println( "Record partition " + record.partition() );
				System.out.println( "Record offset " + record.offset() );
			} );*/
			// commits the offset of record to broker. 
			//consumer.commitAsync();
		//}
		//consumer.close(); /
	}

	static void runProducer(Value v) throws Exception {

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
			Producer<Object, Object> producer = JolieProducerCreator.createProducer(v,keyType,valueType);
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
	}

}
