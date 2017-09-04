package integration.kafka;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.integration.splitter.AbstractMessageSplitter;
import org.springframework.integration.transformer.MessageTransformationException;
import org.springframework.messaging.Message;

public class FileSplitter extends AbstractMessageSplitter{

	private static final Logger logger = LoggerFactory.getLogger(FileSplitter.class);
	
	@Override
	protected Object splitMessage(Message<?> message) {
		// TODO Auto-generated method stub
		Object payload = message.getPayload();
		try {
			
			logger.info(" payload splip : "+ message.toString());
			
			return FileUtils.lineIterator((File) payload);
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			 throw new MessageTransformationException("error split", e);
		}
		
		
	}

}
