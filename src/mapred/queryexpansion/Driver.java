package mapred.queryexpansion;

import java.io.IOException;

import mapred.job.Optimizedjob;
import mapred.querysearch.BM25Mapper;
import mapred.querysearch.BM25Reducer;
import mapred.querysearch.BM25SortMapper;
import mapred.querysearch.BM25SortReducer;
import mapred.querysearch.ScoreComparator;
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
        
        computeDotProducts(input, tmpdir + "/queryExpansion1");
        accumulateDotProducts(tmpdir + "/queryExpansion1", tmpdir + "/queryExpansion2");
        createExpansionMat(tmpdir + "/queryExpansion2", output);

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
	private static void computeDotProducts(String input, String output) 
		throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

        Optimizedjob job = new Optimizedjob(conf, input, output, 
        		"Compute keyword dot products");
        
        job.setClasses(DotProdMapper.class, DotProdReducer.class, null);
        job.setMapOutputClasses(Text.class, Text.class);
        job.run();
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
	private static void accumulateDotProducts(String input, String output) 
		throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();

        Optimizedjob job = new Optimizedjob(conf, input, output,
                "Accumulate dot products");
        
        job.setClasses(ProdAccumMapper.class, ProdAccumReducer.class, null);
        job.setMapOutputClasses(Text.class, DoubleWritable.class);
        job.run();
    }
	
	private static void createExpansionMat(String input, String output) 
			throws IOException, ClassNotFoundException, InterruptedException {
			
			Configuration conf = new Configuration();

	        Optimizedjob job = new Optimizedjob(conf, input, output,
	                "Create query expansion matrix");
	        
	        job.setClasses(ExpandMatMapper.class, ExpandMatReducer.class, null);
	        job.setMapOutputClasses(Text.class, Text.class);
	        job.run();
	    }
	
}
