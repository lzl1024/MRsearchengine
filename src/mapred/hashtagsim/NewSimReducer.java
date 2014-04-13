package mapred.hashtagsim;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class NewSimReducer extends
        Reducer<Text, VIntWritable, Text, VIntWritable> {

    /**
     * Accumulate the partial product sum into the product sum, similar with
     * word count reducer.
     */
    @Override
    protected void reduce(Text key, Iterable<VIntWritable> value, Context context)
            throws IOException, InterruptedException {

        int product = 0;
        for (VIntWritable partialSum : value) {
            product += partialSum.get();
        }

        context.write(key, new VIntWritable(product));
    }
}
