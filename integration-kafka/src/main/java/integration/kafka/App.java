package integration.kafka;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.TaskExecutor;
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
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.support.GenericMessage;


/**
 * Hello world!
 *
 */

@SpringBootApplication
public class App 
{
	
	@Autowired
    public File inboundReadDirectory;
	
	@Autowired
	public File inboundOutDirectory;
	
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
                .transform(Transformers.fileToString())
                .channel(this.INBOUND_CHANNEL)
                .get();
    }
    
    @Bean
    public MessageHandler loggingHandler() {
        LoggingHandler logger = new LoggingHandler("INFO");
        logger.setShouldLogFullMessage(true);
        return logger;
    }
    
    
    @Bean
    public IntegrationFlow writeToFile(@Qualifier("fileWritingMessageHandler") MessageHandler fileWritingMessageHandler) {
        return IntegrationFlows.from(this.INBOUND_CHANNEL)
                //.transform(m -> new StringBuilder((String)m).reverse().toString())
                .handle(fileWritingMessageHandler)
                .split(new FileSplitter())
                .channel("producingChannel")
                .handle(loggingHandler())
                
                .get();
    }
    
    @Bean (name = "fileWritingMessageHandler")
    public MessageHandler fileWritingMessageHandler() {
        FileWritingMessageHandler handler = new FileWritingMessageHandler(inboundOutDirectory);
        handler.setAutoCreateDirectory(true);
        return handler;
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
    	
    }*/
}
