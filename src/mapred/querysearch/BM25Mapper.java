package mapred.querysearch;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BM25Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	static double k1;
	static int N;
	static double b;
	static HashSet<String> querySet;
	
	/**
	 * The key of this mapper is the line number, the value it the word and it vector
	 */
	@Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        
        // parse the word and its vector
        int splitIndex = line.indexOf("\t");
        String word = line.substring(0, splitIndex).trim();
        
        // only handle the word that in the querySet
        if (querySet.contains(word)) {
        	Text output_key = new Text();
        	DoubleWritable output_value = new DoubleWritable();
        	
        	String[] vector = line.substring(splitIndex+1).split(">>");
        	
        	// compute the score and spout the key and value
        	for (String vectorElement : vector) {
        		String[] elements = vectorElement.split(" ");
        		
        		output_key.set(elements[0]);
        		
        		// get BM25 score
        		Double score = Double.parseDouble(elements[1]);
        		output_value.set(computeBM25Score(score, vector.length));
        		//output_value.set(score);
        		
        		context.write(output_key, output_value);
        	}
        }
    }
	
	
	/**
	 * Compute the BM25 score of the particular document
	 * 
	 * @param score
	 * @param vectorLen 
	 * @return
	 */
	private double computeBM25Score(Double score, int vectorLen) {
	    
        //result = score / (double)(score + BM25Mapper.k1 * ((1 - BM25Mapper.b)+
        //        b*(doc_len / avg_doclen));

		double result = Math.log((BM25Mapper.N - vectorLen + 0.5) / (double)(vectorLen + 0.5));
		result *= score / (double)(score + BM25Mapper.k1 * ((1 - BM25Mapper.b)));
	    return result;
    }


	/**
	 * Setup the mapper to read the stopwords from the file
	 */
    @Override
    protected void setup(Context context) {
    	HashMap<String, String> argMap = new HashMap<String, String>();
    	//String argFile = context.getConfiguration().get("args");
    	
    	try {		
    		// run on AWS
            Path p = DistributedCache.getLocalCacheFiles(context.getConfiguration())[0];
            BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
    		
            //BufferedReader reader = new BufferedReader(new FileReader(argFile));
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
    	querySet = new HashSet<String>(Arrays.asList(argMap.get("query").split(" ")));
    	querySet.addAll(Arrays.asList(context.getConfiguration().get("expand").split(" ")));
    }

}
