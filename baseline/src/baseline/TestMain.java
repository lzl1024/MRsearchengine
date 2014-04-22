package baseline;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

public class TestMain {
	static double k1;
	static int N;
	static double b;
	static HashSet<String> querySet;

	private static String argFile = "BM25args.txt";
	private static String inverted_listFile = "inverted_list.txt";
	private static String doc_score = "word_score.txt";
	private static String pick_up_file = "tmp.txt";
	private static int split_num = 10;
	private static String final_output = "result.txt";

	public static void main(String[] args) {
		long time = System.currentTimeMillis();
		readQuery(argFile);

		System.out.println(querySet);

		queryExpansion();
		System.out.println("Expansion time cost:" +  (System.currentTimeMillis() - time));

		querySearch(inverted_listFile, doc_score, pick_up_file);
		System.out.println("Search time cost:" +  (System.currentTimeMillis() - time));
		
		scoreSort(doc_score, split_num, final_output);
		System.out.println("Sort time cost:" +  (System.currentTimeMillis() - time));
	}

	
	// external the sort
	private static void scoreSort(String scoreFile, int splits, String output) {
	    int total_files = 0;
		// internal sort
	    String line = null;
	    BufferedReader reader;
	    PrintWriter writer;
	    
        try {
	        reader = new BufferedReader(new FileReader(scoreFile));

	        // read splits for url and names
		    while (true) {
		    	total_files++;
		    	int counter = 0;
		    	String[] urlNames = new String[splits];
		        double[] scores = new double[splits];
		        writer = new PrintWriter(new FileWriter(total_files + ".txt"));
		    	
		        // get a bunch of records
		        for (; counter < splits && (line = reader.readLine()) != null; counter++) {
			        String[] e = line.split(" ");
		    		urlNames[counter] = e[0];
		    		scores[counter] = Double.parseDouble(e[1]);
		    	}
		    	
		    	// internal sort
		    	for (int i = 0; i < counter - 1; i++) {
		    		for (int j = i+1; j < counter; j++) {
		    			if (scores[i] < scores[j]) {
		    				double tmpscore = scores[i];
		    				scores[i] = scores[j];
		    				scores[j] = tmpscore;
		    				
		    				String tmpurl = urlNames[i];
		    				urlNames[i] = urlNames[j];
		    				urlNames[j] = tmpurl;
		    			}
		    		}
		    	}
		    	
		    	// output internal sort
		    	for (int i = 0; i < counter; i++) {
		    		writer.println(urlNames[i] + " " + scores[i]);
		    	}
		        writer.close();
		    	
		    	if (line == null) {
		    		break;
		    	}
		    }	    
	        
	        reader.close();
	        
	        //external sort
	        ExternalSortHelper.externalSort(total_files, output);
	        
	        // clean up workspace
	        File f = new File(scoreFile);
	        if (f.exists()) {
	        	f.delete();
	        }
	        
        } catch (Exception e) {
	        // TODO Auto-generated catch block
	        e.printStackTrace();
        }  
	    
    }


	// search query based on query set
	private static void querySearch(String invert_list, String output,
	        String tmp) {
		try {
			BufferedReader reader1 = new BufferedReader(new FileReader(
			        invert_list));
			String line;
			PrintWriter writer = new PrintWriter(new FileWriter(tmp));

			// read the argument file of BM25 algorithm and query
			while ((line = reader1.readLine()) != null) {
				// parse the word and its vector
				int splitIndex = line.indexOf("\t");
				String word = line.substring(0, splitIndex).trim();

				// only handle the word that in the querySet
				if (querySet.contains(word)) {
					// write to the tmp file
					writer.println(line);
				}
			}
			reader1.close();
			writer.close();

			// read file from tmp and get query score
			reader1 = new BufferedReader(new FileReader(tmp));
			writer = new PrintWriter(new FileWriter(output));

			while ((line = reader1.readLine()) != null) {
				// parse the word and its vector
				int splitIndex = line.indexOf("\t");
				//String word = line.substring(0, splitIndex).trim();

				String[] vector = line.substring(splitIndex + 1).split(">>");

				for (String vectorElement : vector) {
					// get the single word
					String[] elements = vectorElement.split(" ");

					// go through the tmp file to get the score of the document
					BufferedReader reader2 = new BufferedReader(new FileReader(
					        tmp));
					
					double score = 0;
					String line2;
					while ((line2 = reader2.readLine()) != null) {
						// parse the word and its vector
						int splitIndex2 = line2.indexOf("\t");
						//String word2 = line2.substring(0, splitIndex2).trim();

						String[] vector2 = line2.substring(splitIndex2 + 1).split(">>");

						for (String vectorElement2 : vector2) {
							// get the single doc
							String[] elements2 = vectorElement2.split(" ");
							
							if (elements2[0].equals(elements[0])) {
								// get BM25 score
								double iniscore = Double.parseDouble(elements2[1]);
								score += computeBM25Score(iniscore, vector2.length);
							}
					
						}
					}
					
					writer.println(elements[0] + " " + score);
					reader2.close();
				}

			}

			reader1.close();
			writer.close();
			
	        // clean up workspace
	        File f = new File(tmp);
	        if (f.exists()) {
	        	f.delete();
	        }
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	
	/**
	 * Compute the BM25 score of the particular document
	 * 
	 * @param score
	 * @param vectorLen 
	 * @return
	 */
	private static double computeBM25Score(Double score, int vectorLen) {
		double result = Math.log((N - vectorLen + 0.5) / (double)(vectorLen + 0.5));
		result *= score / (double)(score + k1 * ((1 - b)));
	    return result;
    }
	
	

	// read query from arg file
	protected static void readQuery(String argFile) {
		HashMap<String, String> argMap = new HashMap<String, String>();

		try {
			BufferedReader reader = new BufferedReader(new FileReader(argFile));
			String line;
			// read the argument file of BM25 algorithm and query
			while ((line = reader.readLine()) != null) {
				String[] args = line.split("=");
				if (args.length >= 2) {
					argMap.put(args[0].trim(), args[1].trim());
				}
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// get the arguments
		k1 = Double.parseDouble(argMap.get("k_1"));
		N = Integer.parseInt(argMap.get("N"));
		b = Double.parseDouble(argMap.get("b"));
		querySet = new HashSet<String>(Arrays.asList(argMap.get("query").split(
		        " ")));
	}

	// read the pre-processing word file and pick up what we need according to
	// our query.
	static void queryExpansion() {

	}
}
