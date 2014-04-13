package mapred.hashtagsim;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class HashtagReducer extends Reducer<Text, Text, Text, VIntWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> value, Context context)
            throws IOException, InterruptedException {
        /*
         * Get counter map
         */
        Map<String, Integer> counts = new HashMap<String, Integer>();
        for (Text word : value) {
            String w = word.toString();
            Integer count = counts.get(w);
            if (count == null)
                count = 0;
            count++;
            counts.put(w, count);
        }


        /*
         * Remove those words that only one hashtag included
         */
        if (counts.size() <= 1) {
            return;
        }
        
        Text output_key = new Text();
        VIntWritable output_value = new VIntWritable();

        // copy key value pair into arrays to make nest loop quickly find
        // the start point to spout
        String[] count_keys = new String[counts.size()];
        Integer[] count_values = new Integer[counts.size()];

        int size = 0;
        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            count_keys[size] = entry.getKey();
            count_values[size] = entry.getValue();
            size++;
        }

        // two for loops to spout key pair like: #a #b 1, #b #c 2......
        for (int i = 0; i < size - 1; i++) {
            for (int j = i + 1; j < size; j++) {
                // next line is to debug for the correctness
                //if (count_keys[i].equals("#job") ||
                //count_keys[j].equals("#job")) {

                output_value.set(count_values[i] * count_values[j]);
                // spout in fixed order
                if (count_keys[i].compareTo(count_keys[j]) > 0) {
                    output_key.set(count_keys[i] + " " + count_keys[j]);
                } else {
                    output_key.set(count_keys[j] + " " + count_keys[i]);
                }
                // write into tmp dir
                context.write(output_key, output_value);
                //}
            }
        }
    }
}
