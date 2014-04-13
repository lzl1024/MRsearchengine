package mapred.hashtagsim;

import java.io.IOException;

import mapred.job.Optimizedjob;
import mapred.util.SimpleParser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;

public class Driver {

    public static void main(String args[]) throws Exception {
        SimpleParser parser = new SimpleParser(args);

        String input = parser.get("input");
        String output = parser.get("output");
        String tmpdir = parser.get("tmpdir");

        getHashtagFeatureVector(input, tmpdir + "/feature_vector");

        getHashtagSimilarities(tmpdir + "/feature_vector", output);
    }

    /**
     * Get the hashtags list for all words and spout the hashtag pairs for the
     * next phase of map reduce to accumulate the similarity
     * 
     * @param input
     * @param output
     * @throws Exception
     */
    private static void getHashtagFeatureVector(String input, String output)
            throws Exception {
        Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
                "Get hashtags vector for all words");
        job.setClasses(HashtagMapper.class, HashtagReducer.class, null);
        job.setMapOutputClasses(Text.class, Text.class);
        job.run();
    }

    /**
     * Input is hashtag pairs with its partial similarities, this MapReduce
     * phase will add up of these similarities and get the total similarity
     * 
     * @param input
     * @param output
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    private static void getHashtagSimilarities(String input, String output)
            throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Optimizedjob job = new Optimizedjob(conf, input, output,
                "Get similarities between all hashtags pairs");
        job.setClasses(NewSimMapper.class, NewSimReducer.class,
                NewSimReducer.class);
        job.setMapOutputClasses(Text.class, VIntWritable.class);
        job.run();
    }
}
