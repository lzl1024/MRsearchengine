package mapred.querysearch;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ExpandQueryReducer extends Reducer<Text, VIntWritable, Text, VIntWritable>{

	/**
     * Accumulate the BM25 score of each word in querySet to be the total score, similar with
     * word count reducer.
     */
    @Override
    protected void reduce(Text key, Iterable<VIntWritable> value, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        for (VIntWritable partialSum : value) {
            sum += partialSum.get();
        }

        // output the reduced sums for the queries
        context.write(key, new VIntWritable(sum));
    }
}