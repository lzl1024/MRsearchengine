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
	    String sites = line.substring(splitIndex);
	    
	    String [] pairs = sites.split(">>");
	    
	    for(int i = 0; i < pairs.length; i++){
	    	
	    	String pair = pairs[i].trim();
	    	
	    	// pass empty strings
	    	if(pair.equals("")) continue;
	    	
	    	// split out the site and occurrences
	    	String [] splitPair = sites.split(" ");
	    	
	    	String site = splitPair[0];
	    	String count = splitPair[1];
	    	
	    	StringBuilder sb = new StringBuilder(word);
	    	sb.append(" ");
	    	sb.append(count);
	    	
	    	// output site word/count pair
	    	context.write(new Text(site), new Text(sb.toString()) );
	    }
	}
	
	
}