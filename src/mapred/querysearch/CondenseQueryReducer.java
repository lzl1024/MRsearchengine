package mapred.querysearch;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CondenseQueryReducer extends Reducer<LongWritable, Text, Text, DoubleWritable>{

	/**
     * Accumulate the BM25 score of each word in querySet to be the total score, similar with
     * word count reducer.
     */
    @Override
    protected void reduce(LongWritable key, Iterable<Text> value, Context context)
            throws IOException, InterruptedException {
    	
    	// priority queue will help us to retrieve top 25 queries
    	PriorityQueue<WordCountPair> topQueries = new PriorityQueue<WordCountPair>();
    	
    	for(Text val : value){
    		
    		String [] strPair = value.toString().split(" ");
    		
    		String word = strPair[0];
    		int count = Integer.parseInt(strPair[1]);
    		
    		WordCountPair wcpair = new WordCountPair(word, count);
    		topQueries.add(wcpair);
    	}
    	
    	for(int i = 0; i < 25 || topQueries.size() == 0; i++){
    		
    		WordCountPair output = topQueries.poll();
    		
    		Text output_key = new Text(output.getWord());
    		DoubleWritable output_val = new DoubleWritable(output.getCount());
    		
    		context.write(output_key, output_val);
    	}
    }
    
    class WordCountPair implements Comparable<WordCountPair>{
    	
    	String word;
    	int count;
    	
    	WordCountPair(String word, int count){
    		
    		this.word = word;
    		this.count = count;
    	}
    	
    	public String getWord(){
    		return word;
    	}
    	
    	public int getCount(){
    		return count;
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
