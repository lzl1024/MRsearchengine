package mapred.querysearch;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CondenseQueryMapper extends Mapper<LongWritable, Text, LongWritable, Text>{

	
	/**
	 * The key of this mapper is the line number, the value it the word and it vector
	 */
	@Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
		
		// map all word elements onto the same key
		LongWritable output_key = new LongWritable(1);
		
		// pass along input value unprocessed
		context.write(output_key, value);
		
	}
}
