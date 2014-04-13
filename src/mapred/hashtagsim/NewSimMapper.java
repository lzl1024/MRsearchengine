package mapred.hashtagsim;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class NewSimMapper extends Mapper<LongWritable, Text, Text, VIntWritable> {

    Map<String, Integer> jobFeatures = null;

    /**
     * Directly write the output of the last phase into the reducer, similar
     * with word count mapper
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] keyValuePair = line.split("\t", 2);

        context.write(new Text(keyValuePair[0]),
                new VIntWritable(Integer.parseInt(keyValuePair[1])));
    }
}