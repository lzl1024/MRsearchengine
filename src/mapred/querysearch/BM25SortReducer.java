package mapred.querysearch;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BM25SortReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable>{

    @Override
    protected void reduce(DoubleWritable key, Iterable<Text> value, Context context)
            throws IOException, InterruptedException {
    	// output the key value, restore the key value order
        for (Text urlScore : value) {
        	context.write(urlScore, key);   
        }
    }
}
