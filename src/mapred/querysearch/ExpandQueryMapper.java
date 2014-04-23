package mapred.querysearch;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class ExpandQueryMapper extends Mapper<LongWritable, Text, Text, VIntWritable>{
	
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
        	VIntWritable output_value = new VIntWritable();
        	
        	String[] vector = line.substring(splitIndex+1).split(">>");
        	
        	// output all expanded queries and their values
        	for (String vectorElement : vector) {
        		String[] elements = vectorElement.split(" ");
        		
        		
        		output_key.set(elements[0]);
        		output_value.set(Integer.parseInt(elements[1]));
        		
        		context.write(output_key, output_value);
        	}
        }
		
		
	}
	
	/**
	 * Setup the mapper to read in the query
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
    	querySet = new HashSet<String>(Arrays.asList(argMap.get("query").split(" ")));
    }
}
