import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
	public static long BLOCK_SIZE = 0;
	static Vector<String> tableSchema = new Vector<String>();
	
	private static void parseInput(String[] args) {
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
					tableSchema.add(temp[0]);
					if(temp[1].trim().contains("char")) {
						temp[1] = temp[1].substring(temp[1].indexOf("char(") + 5 , temp[1].length() - 2);
						sizeOfRecord += Long.parseLong(temp[1]);
					}else if(temp[1].trim().contains("date")) {
						sizeOfRecord += 10;
					}else if(temp[1].trim().contains("int")) {
						sizeOfRecord += 10;  //on average int will have 4B in file 
					}else {
						System.out.println("Wrong formatted metadata file");
						System.exit(0);
						break;	
					}
					sizeOfRecord += 1; //for the comma
				}catch(Exception e) {
					System.out.println("Wrong formatted metadata file");
					System.exit(0);
				}
			}
		} catch (IOException e) {
			System.err.println("Unable to read metadata file \"" + metaFile + "\"");
		}
		//Loop will add size for one extra comma after EOL, substract it
		sizeOfRecord -= 1;
		
		//Consider the new line character at the end of line
		sizeOfRecord += 1;
		
		//Get the total number of records that can be fit in a main memory
		BLOCK_SIZE = (mainMemorySize*1024*1024)/sizeOfRecord;
		
		System.out.println("RecordSize : " + sizeOfRecord +"B");
		System.out.println("Block size : " + BLOCK_SIZE);
	}

	private static void beginSort()
	{
		/*	I know that my main memory size is give through command line
		 * 	read the file till the main memory size and sort that data based on the parameter asked
		 * 	how to know that I have read the file till main memory size
		 * 	I need to consider M/R records only to sort into the main memory
		 * 	Total main memory size/single record size = total records that can be fit into the main memory
		 * 	start reading the file and once we reach the "Total records that can be fit into the main memory"
		 * 	sort that data and create a file for that
		 */
		
		long noOfRecordsInFile = 0;
		BufferedReader bfr = null;
		File f = null;
		String line = null;
		int lineCounter = 0, blockCounter = 0;
		ArrayList <String> blockList = null;
		ArrayList <String> intermediateFileList = null;
		
		f = new File(inputFile);
		
		System.out.println(inputFile + " : " + f.length());
		noOfRecordsInFile = f.length() / sizeOfRecord;
		System.out.println("Number of records in file : " + noOfRecordsInFile + "(on average)");
		
		if(noOfRecordsInFile / BLOCK_SIZE > BLOCK_SIZE  - 1) {
			System.err.println("Input file is large. Can cause 3 phase merge sort\nExiting...");
			System.exit(0);
		}
		
		System.out.println("Total intermediate files that can be made : " + (f.length() / sizeOfRecord) / BLOCK_SIZE);
		
		try {			
			bfr = new BufferedReader(new FileReader(new File(inputFile)));
			//Read the records till the BLOCK SIZE - 1
			blockList = new ArrayList<>();
			intermediateFileList = new ArrayList<>();
			while((line = bfr.readLine()) != null ){				
				if(lineCounter == BLOCK_SIZE - 1){
					blockCounter++;
					intermediateFileList.add(sortNCreateTempFile(blockList, blockCounter));
					
					//Reset the variable
					lineCounter = 0;
					blockList.clear();
				}
				blockList.add(line);
				lineCounter++;
			}
			//Check if final lineCounter value is less than the BLOCK SIZE, then create the file for the remaining data 
			if(lineCounter < BLOCK_SIZE - 1){
				blockCounter++;
				intermediateFileList.add(sortNCreateTempFile(blockList, blockCounter));
				lineCounter = 0;
			}
		} catch (IOException e) {
			System.err.println("\""+inputFile+"\" not found	.");
			System.exit(0);
		}
		
		//Printing the file list
		System.out.println("Temporary file list : ");
		for(int i = 0; i < blockCounter; i++){
			System.out.println(intermediateFileList.get(i));
		}
		
		//Sort and merge the intermediate files
		
		
	}
	
	private static String sortNCreateTempFile(ArrayList<String> blockList, int fileCount) {
		String fileName = "i_".concat(String.valueOf(fileCount));
		
		//For sorting integer data
		/*Collections.sort(blockList, new Comparator<String>() {
				@Override
				public int compare(String s1, String s2) {
					return 	Integer.parseInt(s1.split(",")[2]) < Integer.parseInt(s2.split(",")[2]) ? -1 : 
							Integer.parseInt(s1.split(",")[2]) == Integer.parseInt(s2.split(",")[2]) ? 0 : 1;				
		      }
		    });*/
		
		//For sorting date data
		/*Collections.sort(blockList, new Comparator<String>() {
	        	DateFormat f = new SimpleDateFormat("dd/MM/yyyy");
	        	@Override
	        	public int compare(String s1, String s2) {
	            try {
	                return f.parse(s1.split(",")[0]).compareTo(f.parse(s2.split(",")[0]));
	            } catch (ParseException e) {
	                throw new IllegalArgumentException(e);
	            }
	        }
		});*/
		
		//For sorting string data
		Collections.sort(blockList, new Comparator<String>() {
	        	@Override
	        	public int compare(String s1, String s2) {
	                return s1.split(",")[1].compareTo(s2.split(",")[1]);
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

	public static void main(String[] args) 
	{
		//Calculate the execution time
		long startTime = System.currentTimeMillis();
		
		//Parse the input
		parseInput(args);
		
		//Read meta file in hash
		readMetaFile();
		
		//Begin sorting
		beginSort();	
		
		long endTime = System.currentTimeMillis();
		System.out.println("Execution Time : " + (endTime - startTime)/1000 +" sec.");
	}
}