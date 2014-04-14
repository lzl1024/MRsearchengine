package mapred.querysearch;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BM25Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

	/**
     * Accumulate the BM25 score of each word in querySet to be the total score, similar with
     * word count reducer.
     */
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> value, Context context)
            throws IOException, InterruptedException {

        double product = 0;
        for (DoubleWritable partialSum : value) {
            product += partialSum.get();
        }

        context.write(key, new DoubleWritable(product));
    }
}
