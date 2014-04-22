package mapred.queryexpansion;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProdAccumMapper extends Mapper<Text, DoubleWritable, Text, DoubleWritable>{
	
	/**
	 * This mapper is a no op; the key vals are passed unprocessed to the reducer
	 */
	@Override
    protected void map(Text key, DoubleWritable value, Context context)
            throws IOException, InterruptedException {
		
		context.write(key, value);
	}
	
}