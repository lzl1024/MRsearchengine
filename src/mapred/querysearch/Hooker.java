package mapred.querysearch;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.PriorityQueue;

import mapred.queryexpansion.ExpandMatReducer.WordCountPair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Hooker {
	public static String hook(String outputFile) {
		PriorityQueue<WordCountPair> topWords = new PriorityQueue<WordCountPair>();
        StringBuilder builder = new StringBuilder();
        
		try {
	        //BufferedReader reader = new BufferedReader(new FileReader(outputFile));
			//FileSystem fs = FileSystem.get(URI.create(outputFile), new Configuration());
			//BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(outputFile))));
	        
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(outputFile, "part-r-00000"))));
			String line = null;
	        
	        while((line = reader.readLine()) != null) {
	        	String[] record = line.split("\t");
	        	
	        	String word = record[0];
	    		int count = Integer.parseInt(record[1]);
	    		
	    		WordCountPair wc = new WordCountPair(word, count);
	    		
	    		topWords.offer(wc);
	    		// only take the top 25 or until words run out
	    		if (topWords.size() > 25) {
	    			topWords.poll();
	    		}
	        	
	        }
	        
	        while (topWords.size() > 0) {
	        	builder.append(topWords.poll().word);
	        	builder.append(" ");
	        }
	        
	        reader.close();
        } catch (Exception e) {
	        // TODO Auto-generated catch block
	        e.printStackTrace();
        }
		return builder.toString();
	}
	
	public static void main(String[] args) {
		System.out.println(hook("result.txt"));
	}

}
