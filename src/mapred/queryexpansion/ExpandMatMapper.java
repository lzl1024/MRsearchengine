package mapred.queryexpansion;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ExpandMatMapper extends Mapper<Text, DoubleWritable, Text, Text>{
	
	/**
	 * The mapper splits up the product key, allowing both factors to be used as keys and values
	 */
	@Override
    protected void map(Text key, DoubleWritable value, Context context)
            throws IOException, InterruptedException {
		
		// get the count
		String count = value.toString();
		
		// split up the product factors
		String [] prodPair = key.toString().split(" ");
		
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
