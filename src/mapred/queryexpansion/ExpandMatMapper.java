package mapred.queryexpansion;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ExpandMatMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	/**
	 * The mapper splits up the product key, allowing both factors to be used as keys and values
	 */
	@Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
		String line = value.toString();
		
		// parse the word pair and its score
		
        String[] e = line.split("\t");
		// get the count
		String count = e[1];
		
		// split up the product factors
		String [] prodPair = e[0].split(" ");
		
		String wordA = prodPair[0];
		String wordB = prodPair[1];
		
		StringBuilder wordAval = new StringBuilder(prodPair[0]);
		StringBuilder wordBval = new StringBuilder(prodPair[1]);
		
		// set up values
		wordAval.append(" ");
		wordBval.append(" ");
		wordAval.append(count);
		wordBval.append(count);
		
		// both words are keys and values for the other word
		context.write(new Text(wordA), new Text(wordBval.toString()));
		context.write(new Text(wordB), new Text(wordAval.toString()));
	}
	
}
