package mapred.querysearch;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BM25SortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text>{

	/**
	 * The key of this mapper is the line number, the value it the docid and its score
	 */
	@Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] elements = line.split("\\s+");
        
        // just reverse the key value pair
        context.write(new DoubleWritable(Double.parseDouble(elements[1])), new Text(elements[0]));
    }
}
