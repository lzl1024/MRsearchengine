package mapred.hashtagsim;

import java.io.IOException;

import mapred.util.Tokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HashtagMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = Tokenizer.tokenize(line);
        Text output_key = new Text();
        Text output_value = new Text();

        /*
         * Iterate all words, find out all hashtags, then iterate all other
         * non-hashtag words and spout out.
         */
        for (String word : words) {
            if (word.startsWith("#")) {
                for (String word2 : words) {
                    if (!word2.startsWith("#")) {
                        // output "a" #job/n "b" #job....
                        output_key.set(word2);
                        output_value.set(word);
                        context.write(output_key, output_value);
                    }
                }
            }
        }
    }
}
