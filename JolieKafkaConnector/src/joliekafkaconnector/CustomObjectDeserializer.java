/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package joliekafkaconnector;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import jolie.runtime.Value;
import org.apache.kafka.common.serialization.Deserializer;

/**
 *
 * @author maschio
 */
public class CustomObjectDeserializer implements Deserializer<Value>
{
	@Override
	public void configure( Map<String, ?> configs, boolean isKey )
	{
	}

	@Override
	public Value deserialize( String topic, byte[] data )
	{
		ObjectMapper mapper = new ObjectMapper();
		Value object = null;
		try {
			
			object = mapper.readValue( data, Value.class );
			
			
		} catch( Exception exception ) {
			System.out.println( "Error in deserializing bytes " + exception );
		}
		return object;
	}

	@Override
	public void close()
	{
	}



}
