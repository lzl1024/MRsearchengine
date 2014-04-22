package mapred.queryexpansion;

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
        
        computeDotProducts(input, tmpdir + "/queryExpansion1");
        accumulateDotProducts(tmpdir + "/queryExpansion1", tmpdir + "/queryExpansion2");
        createExpansionMat(tmpdir + "/queryExpansion2", output);

    }

	/**
	 * Compute the dot products pieces for word pairs
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
	 * Accumulate dot products to get the similarity score of word pairs
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
        
        job.setClasses(ProdAccumMapper.class, ProdAccumReducer.class, ProdAccumReducer.class);
        job.setMapOutputClasses(Text.class, VIntWritable.class);
        job.run();
    }
	
	
	/**
	 * Create query expansion matrix 
	 *  
	 * @param input
	 * @param output
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
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
