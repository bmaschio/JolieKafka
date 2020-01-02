/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package joliekafkaconnector;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Reader;
import java.io.StringReader;
import java.net.ConnectException;
import java.util.Map;

import com.fasterxml.jackson.databind.type.TypeFactory;
import jolie.js.JsUtils;
import jolie.runtime.Value;
import org.apache.kafka.common.serialization.Deserializer;

/**
 *
 * @author maschio
 */
public class ValueJolieDeserializer implements Deserializer<Value>
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
			String s = mapper.readValue(data,String.class);
			Reader rd  = new StringReader(s);
			JsUtils.parseJsonIntoValue(rd,object,false);
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
