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

	  @Value("${kafka.bootstrap-servers}")
	  private String bootstrapServers;

	  private String INBOUND_PATH="/home/joset/data";
	  
	  @Bean
	  public DirectChannel producingChannel() {
	    return new DirectChannel();
	  }

	  @Bean
	  @ServiceActivator(inputChannel = "producingChannel")
	  public MessageHandler kafkaMessageHandler() {
	    KafkaProducerMessageHandler<String, String> handler =
	        new KafkaProducerMessageHandler<>(kafkaTemplate());
	    handler.setMessageKeyExpression(new LiteralExpression("kafka-integration"));

	    return handler;
	  }

	  @Bean
	  public KafkaTemplate<String, String> kafkaTemplate() {
	    return new KafkaTemplate<>(producerFactory());
	  }

	  @Bean
	  public ProducerFactory<String, String> producerFactory() {
	    return new DefaultKafkaProducerFactory<>(producerConfigs());
	  }
	  
	  @Bean
	  public Map<String, Object> producerConfigs() {
	    Map<String, Object> properties = new HashMap<>();
	    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
	    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
	    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
	    // introduce a delay on the send to allow more messages to accumulate
	    properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);

	    return properties;
	  }
	  
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
	  
	 
}
