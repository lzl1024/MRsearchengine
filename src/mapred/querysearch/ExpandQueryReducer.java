package mapred.querysearch;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ExpandQueryReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

	/**
     * Accumulate the BM25 score of each word in querySet to be the total score, similar with
     * word count reducer.
     */
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> value, Context context)
            throws IOException, InterruptedException {

        double sum = 0;
        for (DoubleWritable partialSum : value) {
            sum += partialSum.get();
        }

        // output the reduced sums for the queries
        context.write(key, new DoubleWritable(sum));
    }
}