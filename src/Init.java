import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Vector;

/**
 * @author ganesh
 *
 */

public class Init 
{
	public static String metaFile, inputFile, outputFile, sortOrder;
	public static Vector <String> outputColumnList;
	public static Vector <String> sortColumnList;
	public static long mainMemorySize;
	public static long sizeOfRecord = 0;
	static HashMap<String, String> tableSchema = new HashMap<String, String>();
	
	private static void ___parseInput(String[] args) {
		/*	Parse the command line arguments here and make them static 
		 * 	so that they can be used anywhere in the program  
		 */
		
		/*	Command line format : 
		 *  "./mysort --­­meta_file metafile.txt --­­input_file input.txt ­­--output_file output.txt --­­output_column 
		 *	col1,col2 --­­sort_column col1,col2 --­­mm 1040 ­­--order asc"		  
		 */

		int argLength = args.length;
		
		//Check for the count
		if(argLength != 14) {
			 System.err.println("Error!!! Invalid arguments.");
			 System.err.println("Usage : /n\"Init --meta_file <fileName> --input_file <fileName> ­--output_file <fileName> " +
			 					 "­--output_column <col1,col2,...> --sort_column <col1,col2,...> --mm <size in MB> --order <asc/desc>\"");
			 System.exit(0);
		}
		
		//Extract the command line switches
		for(int i = 0; i < argLength; i++) {
			switch(args[i].trim()) {
				case "--meta_file":
						metaFile = args[++i].trim();
						break;
				case "--input_file":
						inputFile = args[++i].trim();
						break;
				case "--output_file":
						outputFile = args[++i].trim();
						break;
				case "--output_column":
						String tempList[] = args[++i].trim().split(",");
						outputColumnList = new Vector <String>();
						for(int j = 0; j < tempList.length; j++)
							outputColumnList.add(tempList[j]);
						break;
				case "--sort_column":
						String tempList2[] = args[++i].trim().split(",");
						sortColumnList = new Vector <String>();
						for(int j = 0; j < tempList2.length; j++)
							sortColumnList.add(tempList2[j]);
						break;
				case "--mm":
						mainMemorySize = Integer.parseInt(args[++i].trim());
						break;
				case "--order":
						sortOrder = args[++i];
						break;
				default:
						System.err.println("Error in arguments...");
						System.exit(0);
						break;
			}
		}
		System.out.println("metaFileName : " + metaFile );
		System.out.println("inputFile : " + inputFile );
		System.out.println("outputFile : " + outputFile );
		System.out.println("outputColumnList : " + outputColumnList.toString());
		System.out.println("sortColumnList : " + sortColumnList.toString() );
		System.out.println("mainMemorySize : " + mainMemorySize );
		System.out.println("sortOrder : " + sortOrder );
	}
	
	public static void readMetaFile()
	{
		File f = new File(metaFile);
		BufferedReader freader = null;
		try {
			freader = new BufferedReader(new FileReader(f));
		} catch (FileNotFoundException e) {
			System.err.println("Unable to read metadata file \"" + metaFile + "\"");
		}
		String line = null;
		try {
			while((line = freader.readLine()) != null) {
				String temp[] = line.split(",");
				try{
					tableSchema.put(temp[0],temp[1]);
					if(temp[1].trim().contains("char")) {
						temp[1] = temp[1].substring(temp[1].indexOf("char(") + 5 , temp[1].length() - 2);
						sizeOfRecord += (Long.parseLong(temp[1]) * 2); //Character is 2B in java
					}else if(temp[1].trim().contains("date")) {
						sizeOfRecord += (6 * 2);  //Character is 2B in java
					}else if(temp[1].trim().contains("int")) {
						sizeOfRecord += 4;  //Int is 4B in java
					}else {
						System.out.println("Wrong formatted metadata file");
						System.exit(0);
						break;	
					}
					sizeOfRecord += 2; //for the comma
				}catch(Exception e) {
					System.out.println("Wrong formatted metadata file");
					System.exit(0);
				}
			}
		} catch (IOException e) {
			System.err.println("Unable to read metadata file \"" + metaFile + "\"");
		}
		//Loop will add size for one extra comma after EOL, substract it
		sizeOfRecord -= 2;
		
		System.out.println("File Size : " + f.length());
		System.out.println("RecordSize : " + sizeOfRecord +"B");
	}
	
	public static void main(String[] args) 
	{
		//Calculate the execution time
		long startTime = System.currentTimeMillis();
		
		//Parse the input
		___parseInput(args);
		
		//Read meta file in hash
		readMetaFile();
		
		//Begin sorting
		
			
		
		long endTime = System.currentTimeMillis();
		System.out.println("Execution Time : " + (endTime - startTime)/1000 +" sec.");
	}
}