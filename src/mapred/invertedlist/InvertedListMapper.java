package mapred.invertedlist;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class InvertedListMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	static HashSet<String> stopwordSet;

	/**
	 * The key of this mapper is the line number, the value it the content of the abstract
	 */
	@Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        
        // filter for commented line
        if (line.startsWith("#")) {
        	return;
        }
        Text output_value = null;
        String[] words = line.split("\\s+");
        Text output_key = new Text();
        
        // get the particular page
        int pos = words[0].lastIndexOf("/");
        
        
        // get the source keyword
        output_value = new Text(words[0].substring(pos+1, words[0].length()-1));
        // handle with the first \" and the last \."
        words[2] = words[2].substring(1);
        
        int cutPos = words[words.length - 1].lastIndexOf("\"");      
        words[words.length - 2] = cutPos <= 1 ? "" : words[words.length - 2].
        		substring(0, cutPos - 1);
        
        // output the key and value of the output vector
        for (int i = 2; i < words.length; i++) {
        	// remove the punctuation in the front and the back
        	//String word = words[i].replaceFirst("^[^a-zA-Z0-9]+", "");
        	//word = word.replaceAll("[^a-zA-Z0-9]+$", "");
        	String word = words[i].toLowerCase().trim();
        	if (word.matches("[a-z]+") && !stopwordSet.contains(word)) {
        		output_key.set(word);
        		context.write(output_key, output_value);
        	}
        }
    }
	
	
	/**
	 * Setup the mapper to read the stopwords from the file
	 */
    @Override
    protected void setup(Context context) {
    	stopwordSet = new HashSet<String>();
    	stopwordSet.add("");
    	//String stopword = context.getConfiguration().get("stopword");
    	
    	try {
    		// run on AWS
            Path p = DistributedCache.getLocalCacheFiles(context.getConfiguration())[0];
            BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
            
	        //BufferedReader reader = new BufferedReader(new FileReader(stopword));
	        String line;
	        // read the stopword file and get a hashset of stopword
	        while ((line = reader.readLine()) != null) {
	        	String[] stopwords = line.split(",");
	        	for (String word : stopwords) {
	        		stopwordSet.add(word.trim());
	        	}
	        }

	        reader.close();
        } catch (Exception e) {
	        e.printStackTrace();
        }
    }
}
