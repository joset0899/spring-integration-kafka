package integration.kafka;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.integration.transformer.GenericTransformer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.messaging.MessageHandler;



import org.springframework.integration.dsl.core.Pollers;

import org.springframework.integration.dsl.support.Transformers;

@Configuration
public class ProducerChanelKafka {

	  

	  private String INBOUND_PATH="/home/joset/data";
	  
	  
	  /*
	  @Bean 
	  GenericTransformer<String, Object> transformer() { 
	   return new GenericTransformer<String, Object>() { 
	    @Override 
	    public Foo transform(String payload) { 
	     
	    	String[] cadena = payload.split(",");
	    	  Foo obj = new Foo();
	    	  obj.setBar(cadena[0]);
	    	  obj.setFoo(cadena[1]);
	    	
	    	return  obj; 
	    } 
	   }; 
	  } 
	  */
	 
}
