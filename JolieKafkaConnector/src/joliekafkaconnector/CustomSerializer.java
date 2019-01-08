/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package joliekafkaconnector;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import jolie.runtime.Value;
import org.apache.kafka.common.serialization.Serializer;
/**
 *
 * @author maschio
 */
public class CustomSerializer implements Serializer<Value> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }
	
    @Override
    public byte[] serialize(String topic, Value data) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
        retVal = objectMapper.writeValueAsBytes( data );
			
        } catch (Exception exception) {
        System.out.println("Error in serializing object"+ data);
        }
        return retVal;
    }

	@Override
	public void close()
	{
		throw new UnsupportedOperationException( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
	}


	
}
