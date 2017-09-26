package integration.kafka;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.integration.transformer.GenericTransformer;
import com.monitorjbl.xlsx.*;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TransformerXlsToObject implements GenericTransformer<File,List<Foo>> {

	@Override
	public List<Foo> transform(File file) {
		// TODO Auto-generated method stub
		
		List<Foo> lista = new ArrayList<>();
		List<String> listLinea = new ArrayList<>();
		
		try (Stream<String> stream = Files.lines(Paths.get(file.getAbsolutePath()))) {

			//1. filter line 3
			//2. convert all content to upper case
			//3. convert it into a List
			listLinea = stream					
					.collect(Collectors.toList());
			
			listLinea.stream().forEach(linea -> {
				
				String[] campos = linea.split(",");
				Foo foo = new Foo();
				try {
					
					//if(campos.length==1)
			        foo.setCampo1(campos[0]);
					//if(campos.length==2)
			        foo.setCampo2(campos[1]);
					//if(campos.length==3)
			        foo.setCampo3(campos[2]);
					//if(campos.length==4)
			        foo.setCampo4(campos[3]);
					//if(campos.length==5)
			        foo.setCampo5(campos[4]);
					//if(campos.length==6)
			        foo.setCampo6(campos[5]);
					//if(campos.length==7)
			        foo.setCampo7(campos[6]);
					//if(campos.length==8)
			        foo.setCampo8(campos[7]);
					//if(campos.length==9)
			        foo.setCampo9(campos[8]);
					//if(campos.length==10)
			        foo.setCampo10(campos[9]);
					//if(campos.length==11)
			        foo.setCampo11(campos[10]);
					//if(campos.length==12)
			        foo.setCampo12(campos[11]);
					//if(campos.length==13)
			        foo.setCampo13(campos[12]);
					//if(campos.length==14)
			        foo.setCampo14(campos[13]);
					//if(campos.length==15)
			        foo.setCampo15(campos[14]);
			    	lista.add(foo);
					
				} catch (Exception e) {
					// TODO: handle exception
				}
				
				
			});

		} catch (IOException e) {
			e.printStackTrace();
		}
		
		/*try 
		    (
				  InputStream is = new FileInputStream(file);
				  Workbook workbook = StreamingReader.builder()
				          .rowCacheSize(100)
				          .bufferSize(4096)
				          .open(is)) {
				  for (Sheet sheet : workbook){
				    System.out.println(sheet.getSheetName());
				    for (Row r : sheet) {
				    	
				    	Foo foo = new Foo();
				        foo.setCampo1(r.getCell(0).getStringCellValue());
				        foo.setCampo2(r.getCell(1).getStringCellValue());
				        foo.setCampo3(r.getCell(2).getStringCellValue());
				        foo.setCampo4(r.getCell(3).getStringCellValue());
				        foo.setCampo5(r.getCell(4).getStringCellValue());
				        foo.setCampo6(r.getCell(5).getStringCellValue());
				        foo.setCampo7(r.getCell(6).getStringCellValue());
				        foo.setCampo8(r.getCell(7).getStringCellValue());
				        foo.setCampo9(r.getCell(8).getStringCellValue());
				        foo.setCampo10(r.getCell(9).getStringCellValue());
				        foo.setCampo11(r.getCell(10).getStringCellValue());
				        foo.setCampo12(r.getCell(11).getStringCellValue());
				        foo.setCampo13(r.getCell(12).getStringCellValue());
				        foo.setCampo14(r.getCell(13).getStringCellValue());
				        foo.setCampo15(r.getCell(14).getStringCellValue());
				    	lista.add(foo);
				      
				    }
				  }
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}catch (Exception e) {
					// TODO: handle exception
					e.printStackTrace();
				}*/
		
		
		return lista;
	}

}
