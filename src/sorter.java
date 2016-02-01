import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;


public class sorter {

	public static String sortNCreateTempFile(ArrayList<String> blockList, int fileCount) {
		
		String fileName = "i_".concat(String.valueOf(fileCount));
		Collections.sort(blockList, new Comparator<String>(){

			@Override
			public int compare(String s1, String s2){
				int indexToSort = 0;
				Init init = new Init();
				for(int i=0;i<Init.sortColumnList.size();i++)
				{
					System.out.println("indexToSort : " + init.tableColNames.get(init.sortColumnList.get(i)));
					System.out.println("index : " + init.tableColDataTypes.get(i));
					indexToSort = init.tableColNames.get(init.sortColumnList.get(i));
					if(!s1.split(",")[indexToSort].equals(s2.split(",")[indexToSort])){
						break;
					}
				}
				switch(init.tableColDataTypes.get(indexToSort)) {
					case "int" :
						if(Integer.parseInt(s1.split(",")[indexToSort]) < Integer.parseInt(s2.split(",")[indexToSort]))
							return -1;
						else if(Integer.parseInt(s1.split(",")[indexToSort]) == Integer.parseInt(s2.split(",")[indexToSort]))
							return 0;
						else
							return 1;
					case "char" :
						return s1.split(",")[indexToSort].compareTo(s2.split(",")[indexToSort]);
					case "date" :
						DateFormat f = new SimpleDateFormat("dd/MM/yyyy");
						try {
							return f.parse(s1.split(",")[indexToSort]).compareTo(f.parse(s2.split(",")[indexToSort]));
						} catch (ParseException e1) {
							e1.printStackTrace();
						}	
				}
				return 0;
			}	
		});
				
		FileWriter fwr = null;
		//create a intermediate file
		try {
			fwr = new FileWriter(fileName);
			for(int i = 0; i < blockList.size(); i++){
				fwr.write(blockList.get(i)+"\n");
			}
			fwr.close();
		} catch (IOException e) {
			e.printStackTrace();
		}		
		return fileName;
	}
}