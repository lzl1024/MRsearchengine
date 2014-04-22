package mapred.queryexpansion;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ExpandMatReducer extends Reducer<Text, Text, Text, Text>{
	
	/**
	 * Accumulate 25 top words associated with keyword
     */
    @Override
    protected void reduce(Text key, Iterable<Text> value, Context context)
            throws IOException, InterruptedException {
    	
    	PriorityQueue<WordCountPair> topWords = new PriorityQueue<WordCountPair>();
    	
    	// extract and sort words
    	for(Text pair : value){
    		
    		String [] wordCount = pair.toString().split(" ");
    		
    		String word = wordCount[0];
    		int count = Integer.parseInt(wordCount[1]);
    		
    		WordCountPair wc = new WordCountPair(word, count);
    		
    		topWords.offer(wc);
    		// only take the top 25 or until words run out
    		if (topWords.size() > 25) {
    			topWords.poll();
    		}
    	}
    	
    	// create list of sorted pairings
    	StringBuilder output_value = new StringBuilder();
    	
    	while (topWords.size() > 0) {
    		output_value.append(topWords.poll() + ">>");
    		
    	}
    	
    	// output list
    	context.write(key, new Text(output_value.toString()));
    	
    }
    
    class WordCountPair implements Comparable<WordCountPair>{
    	
    	String word;
    	int count;
    	
    	WordCountPair(String word, int count){
    		
    		this.word = word;
    		this.count = count;
    	}
    	
    	public String toString(){
    		
    		StringBuilder sb = new StringBuilder(word);
    		sb.append(" ");
    		sb.append(count);
    		
    		return sb.toString();	
    	}
    	
    	public int compareTo(WordCountPair that){
    		
    		// opposite of natural ordering to create a max heap
    		return (this.count - that.count);
    		
    	}
    }
}
