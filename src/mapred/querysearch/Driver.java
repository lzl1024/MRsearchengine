package mapred.querysearch;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import mapred.job.Optimizedjob;
import mapred.util.SimpleParser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Driver {
	public static void main(String args[]) throws Exception {
        SimpleParser parser = new SimpleParser(args);

        String input = parser.get("input");
        String output = parser.get("output");
        String expandInput = parser.get("expIn");
        String expandOutput = parser.get("expOut");
        String tmpdir = parser.get("tmpdir");
        String arguments = parser.get("args");
        
        expandQuery(expandInput, tmpdir + "/expand", arguments);
        condenseQuery(tmpdir + "/expand", expandOutput);
        getBM25Score(input, tmpdir + "/bm25score", arguments);
        getRankedDocs(tmpdir + "/bm25score", output);

    }

	
	/**
	 * Expands query input by the user
	 * 
	 * @param input
	 * @param output
	 * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
	 */
	private static void expandQuery(String input, String output, String arguments) 
		throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		conf.set("args", arguments);

        Optimizedjob job = new Optimizedjob(conf, input, output,
                "expand user given query");
        
        job.setClasses(ExpandQueryMapper.class, ExpandQueryReducer.class,
        		null);
        job.setMapOutputClasses(Text.class, DoubleWritable.class);
        job.run();
    }
	
	/**
	 * Condenses query after expanded to 25 best words
	 * 
	 * @param input
	 * @param output
	 * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
	 */
	private static void condenseQuery(String input, String output) 
		throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();

        Optimizedjob job = new Optimizedjob(conf, input, output,
                "condense query again");
        
        job.setClasses(CondenseQueryMapper.class, CondenseQueryReducer.class,
        		null);
        job.setMapOutputClasses(LongWritable.class, Text.class);
        job.run();
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
		Configuration conf = new Configuration();

        Optimizedjob job = new Optimizedjob(conf, input, output,
                "Sort BM25 score");
        
        job.setClasses(BM25SortMapper.class, BM25SortReducer.class, null);
        job.setMapOutputClasses(DoubleWritable.class, Text.class);
        job.setSortComparatorClass(ScoreComparator.class);
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
	private static void getBM25Score(String input, String output, String arguments) 
		throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		conf.set("args", arguments);
		
    	try {
	        DistributedCache.addCacheFile(new URI(arguments), conf);
        } catch (URISyntaxException e) {
	        // TODO Auto-generated catch block
	        e.printStackTrace();
        }

        Optimizedjob job = new Optimizedjob(conf, input, output,
                "Get BM25 score of all docs");
        
        // Combiner is the same as Reducer
        job.setClasses(BM25Mapper.class, BM25Reducer.class,
        		BM25Reducer.class);
        job.setMapOutputClasses(Text.class, DoubleWritable.class);
        job.run();
    }
}
