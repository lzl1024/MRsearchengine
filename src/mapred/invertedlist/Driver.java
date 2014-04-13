package mapred.invertedlist;

import mapred.job.Optimizedjob;
import mapred.util.SimpleParser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

public class Driver {
	 public static void main(String args[]) throws Exception {
	        SimpleParser parser = new SimpleParser(args);

	        String input = parser.get("input");
	        String output = parser.get("output");
	        String stopword = parser.get("stopword");

	        getInvertedList(input, output, stopword);
	    }

	    /**
	     * Get the inverted list of the document set from DBpedia's dataset
	     * 
	     * @param input
	     * @param output
	     * @throws Exception
	     */
	    private static void getInvertedList(String input, String output, String stopword)
	            throws Exception {
	    	Configuration config = new Configuration();
	    	config.set("stopword", stopword);
	        Optimizedjob job = new Optimizedjob(config, input, output,
	                "Get inverted list of the document set");
	        job.setClasses(InvertedListMapper.class, InvertedListReducer.class, null);
	        job.setMapOutputClasses(Text.class, Text.class);
	        job.run();
	    }
}
