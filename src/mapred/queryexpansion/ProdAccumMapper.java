package mapred.queryexpansion;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class ProdAccumMapper extends Mapper<LongWritable, Text, Text, VIntWritable>{
	
	/**
	 * This mapper is a no op; the key vals are passed unprocessed to the reducer
	 */
	@Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
		String line = value.toString();
		
		// parse the word pair and its score
        String[] e = line.split("\t");
		
		context.write(new Text(e[0]), new VIntWritable(Integer.parseInt(e[1])));
	}
	
}