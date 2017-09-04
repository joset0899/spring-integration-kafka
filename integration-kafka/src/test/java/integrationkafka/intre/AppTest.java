package integrationkafka.intre;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

import integration.kafka.CountDownLatchHandler;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Unit test for simple App.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class AppTest   
{
   
	private static final Logger LOGGER =
		      LoggerFactory.getLogger(AppTest.class);

		  @Autowired
		  private ApplicationContext applicationContext;

		  @Autowired
		  private CountDownLatchHandler countDownLatchHandler;

		  private static String SPRING_INTEGRATION_KAFKA_TOPIC = "spring-integration-kafka.t";

		  @ClassRule
		  public static KafkaEmbedded embeddedKafka =
		      new KafkaEmbedded(1, true, SPRING_INTEGRATION_KAFKA_TOPIC);

		  @Test
		  public void testIntegration() throws Exception {
		    MessageChannel producingChannel =
		        applicationContext.getBean("producingChannel", MessageChannel.class);

		    Map<String, Object> headers = new HashMap<>();
		    headers.put(KafkaHeaders.TOPIC, SPRING_INTEGRATION_KAFKA_TOPIC);
		      //  Collections.singletonMap(KafkaHeaders.TOPIC, SPRING_INTEGRATION_KAFKA_TOPIC);

		    LOGGER.info("sending 10 messages");
		    for (int i = 0; i < 10; i++) {
		      GenericMessage<String> message =
		          new GenericMessage<>("Hello Spring Integration Kafka " + i + "!", headers);
		      producingChannel.send(message);
		      LOGGER.info("sent message='{}'", message);
		    }

		    countDownLatchHandler.getLatch().await(10000, TimeUnit.MILLISECONDS);
		    assertThat(countDownLatchHandler.getLatch().getCount()).isEqualTo(0);
		  }
	
}
