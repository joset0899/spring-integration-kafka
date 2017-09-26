package integration.kafka;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.dsl.support.GenericHandler;

public class DeleteFileHandler implements GenericHandler<File> {

	@Override
	public Object handle(File payload, Map<String, Object> headers){
		// TODO Auto-generated method stub
		
		File fileTmp = null;
		
		try {
			
			fileTmp = File.createTempFile(IntegrationMessageHeaderAccessor.SEQUENCE_NUMBER, ".data");
			if(payload.renameTo(fileTmp)){
				System.out.println("moved operation is success.");
				if(payload.delete()){
					System.out.println(payload.getName() + " is deleted!");
				}else{
					System.out.println("Delete operation is failed.");
				}
				
			}else{
				
				System.out.println("moved operation is failed.");
			}
				
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return fileTmp;
	}

}
