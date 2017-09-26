package integration.kafka;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.core.task.TaskExecutor;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.channel.MessageChannels;
import org.springframework.integration.dsl.core.Pollers;
import org.springframework.integration.dsl.support.Transformers;
import org.springframework.integration.file.FileReadingMessageSource;
import org.springframework.integration.file.FileWritingMessageHandler;
import org.springframework.integration.file.filters.AcceptOnceFileListFilter;
import org.springframework.integration.file.filters.CompositeFileListFilter;
import org.springframework.integration.file.filters.RegexPatternFileListFilter;
import org.springframework.integration.file.filters.SimplePatternFileListFilter;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.support.GenericMessage;


/**
 * Hello world!
 *
 */

@SpringBootApplication
@ComponentScan
@Import(ProducerChanelKafka.class)
public class App 
{
	
	@Autowired
    public File inboundReadDirectory;
	
	@Autowired
	public File inboundOutDirectory;
	
	@Value("${kafka.bootstrap-servers}")
	  private String bootstrapServers;
	
	private static String SPRING_INTEGRATION_KAFKA_TOPIC = "spring-integration-kafka";
	
    public static void main( String[] args )
    {
    	ConfigurableApplicationContext context = new SpringApplicationBuilder(App.class)
    			.web(false)
    			.run(args);
    	//context.getBean(App.class).runDemo(context);
		//context.close();
    }
    
    public static final String INBOUND_CHANNEL = "inbound-channel";


    @Bean(name = INBOUND_CHANNEL)
    public MessageChannel inboundFilePollingChannel() {
        return MessageChannels.direct().get();
    }
    
    @Bean
    public FileReadingMessageSource fileReadingMessageSource(@Value("${inbound.filename.regex}") String regex) {
        FileReadingMessageSource source = new FileReadingMessageSource();
        source.setDirectory(this.inboundReadDirectory);
        source.setAutoCreateDirectory(true);
        /*CompositeFileListFilter<File> filter = new CompositeFileListFilter<>(
                Arrays.asList(new AcceptOnceFileListFilter<File>(),
                        new RegexPatternFileListFilter("*.txt"))
        );*/
        source.setFilter(new SimplePatternFileListFilter("*.txt"));
        return source;
    }
    
    @Bean
    public IntegrationFlow inboundFileIntegration(@Value("${inbound.file.poller-fixed-delay}") long period,
                                                  @Value("${inbound.file.poller-max-messages-per-poll}") int maxMessagesPerPoll,
                                                   MessageSource<File> fileReadingMessageSource) {
        return IntegrationFlows.from(fileReadingMessageSource,
                c -> c.poller(Pollers.fixedDelay(period)
                        .maxMessagesPerPoll(maxMessagesPerPoll)
                        ))
        		.handle(new DeleteFileHandler())
                
                .channel(this.INBOUND_CHANNEL)
                .get();
    }
    
    @Bean
    public MessageHandler loggingHandler() {
        LoggingHandler logger = new LoggingHandler("INFO");
        logger.setShouldLogFullMessage(true);
        return logger;
    }
    
    public TransformerXlsToObject transformerXlsToObject(){
    	return new TransformerXlsToObject();
    }
    
    @Bean
    public IntegrationFlow writeToFile(@Qualifier("fileWritingMessageHandler") MessageHandler fileWritingMessageHandler) {
        return IntegrationFlows.from(this.INBOUND_CHANNEL)
                //.transform(m -> new StringBuilder((String)m).reverse().toString())
                .handle(fileWritingMessageHandler)
                .transform(transformerXlsToObject())
                .split()
                //.channel("producingChannel2")
                .handle(kafkaMessageHandler())
                //.handle(loggingHandler())
                
                .get();
    }
    
    @Bean (name = "fileWritingMessageHandler")
    public MessageHandler fileWritingMessageHandler() {
        FileWritingMessageHandler handler = new FileWritingMessageHandler(inboundOutDirectory);
        handler.setAutoCreateDirectory(true);
        return handler;
    }
    
    @Bean
	  public DirectChannel producingChannel() {
	    return new DirectChannel();
	  }

	  @Bean
	  @ServiceActivator(inputChannel = "producingChannel")
	  public MessageHandler kafkaMessageHandler() {
	    KafkaProducerMessageHandler<String, Foo> handler =
	        new KafkaProducerMessageHandler<>(kafkaTemplate());
	    handler.setMessageKeyExpression(new LiteralExpression("kafka-integration"));
	    handler.setTopicExpression(new LiteralExpression(SPRING_INTEGRATION_KAFKA_TOPIC));

	    return handler;
	  }

	  @Bean
	  public KafkaTemplate<String, Foo> kafkaTemplate() {
	    return new KafkaTemplate<>(producerFactory());
	  }

	  @Bean
	  public ProducerFactory<String, Foo> producerFactory() {
	    return new DefaultKafkaProducerFactory<>(producerConfigs());
	  }
	  
	  @Bean
	  public Map<String, Object> producerConfigs() {
	    Map<String, Object> properties = new HashMap<>();
	    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
	    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
	    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
	    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
	    //properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
	    // introduce a delay on the send to allow more messages to accumulate
	    properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);

	    return properties;
	  }
    
    /*
    private void runDemo(ConfigurableApplicationContext context) {
    	
    	MessageChannel producingChannel =
    			context.getBean("producingChannel", MessageChannel.class);
    	
    	System.out.println("Sending 10 messages...");
    	
    	Map<String, Object> headers = new HashMap<>();
	    headers.put(KafkaHeaders.TOPIC, SPRING_INTEGRATION_KAFKA_TOPIC);
	    
	    
	    for (int i = 0; i < 10; i++) {
	      GenericMessage<String> message =
	          new GenericMessage<>("Hello Spring Integration Kafka " + i + "!", headers);
	      producingChannel.send(message);
	      System.out.println("sent message='{}'"+ message);
	    }
    	
    }
   */
}
