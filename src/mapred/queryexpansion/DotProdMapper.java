package mapred.queryexpansion;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DotProdMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	
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
	    String sites = line.substring(splitIndex+1);
	    Text output_key = new Text();
	    Text output_value = new Text();
	    
	    String [] pairs = sites.split(">>");
	    
	    for(String pair : pairs){	    	
	    	// split out the site and occurrences
	    	String [] splitPair = pair.split(" ");
	    	
	    	// set output_key to be the site
	    	output_key.set(splitPair[0]);
	    	String count = splitPair[1];
	    	
	    	StringBuilder sb = new StringBuilder(word);
	    	sb.append(" ");
	    	sb.append(count);
	    	output_value.set(sb.toString());
	    	
	    	// output site word/count pair
	    	context.write(output_key, output_value);
	    }
	}
	
	
}