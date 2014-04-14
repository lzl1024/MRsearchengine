package mapred.querysearch;

import java.io.IOException;

import mapred.job.Optimizedjob;
import mapred.util.SimpleParser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class Driver {
	public static void main(String args[]) throws Exception {
        SimpleParser parser = new SimpleParser(args);

        String input = parser.get("input");
        String output = parser.get("output");
        String tmpdir = parser.get("tmpdir");
        String arguments = parser.get("args");
        
        getBM25Score(input, tmpdir + "/bm25score", arguments);
        getRankedDocs(tmpdir + "/bm25score", output);

    }

	/**
	 * Sort the score of document to get the final rank of the documents
	 * 
	 * @param input
	 * @param output
	 * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
	 */
	private static void getRankedDocs(String input, String output) 
		throws IOException, ClassNotFoundException, InterruptedException {
	    // TODO Auto-generated method stub
	    
    }

	
	/**
	 * Compute the BM25 score of each Document and output to the tmp
	 * file
	 * 
	 * @param input
	 * @param output
	 * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
	 */
	private static void getBM25Score(String input, String output, String arguments) 
		throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		conf.set("args", arguments);

        Optimizedjob job = new Optimizedjob(conf, input, output,
                "Get BM25 score of all docs");
        
        // Combiner is the same as Reducer
        job.setClasses(BM25Mapper.class, BM25Reducer.class,
        		BM25Reducer.class);
        job.setMapOutputClasses(Text.class, DoubleWritable.class);
        job.run();
	    
    }
}
