package mapred.ngramcount;

import java.io.IOException;
import java.util.LinkedList;

import mapred.util.Tokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NgramCountMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] words = Tokenizer.tokenize(line);

		// get command args, default is 1
		int gramSize = 1;

		Configuration conf = context.getConfiguration();

		String gramSizeArg = conf.get("n");
		
		if(gramSizeArg != null){
			
			if(gramSizeArg.length() != 0){
				gramSize = Integer.parseInt(gramSizeArg);
			}
			
		}
		
		// this queue will hold the ngrams
		LinkedList<String> wordQueue = new LinkedList<String>();
		
		for (int n = 0; n < words.length; n++){
			
			wordQueue.offer(words[n]);
			
			// once the queue has reached the size of the ngram, start printing out the grams
			if(wordQueue.size() == gramSize){
				
				// write the current queue to a string
				StringBuilder currentGram = new StringBuilder();
				
				for(String word : wordQueue){
					
					currentGram.append(word);
					currentGram.append(" ");
					
				}
				
				// write out the gram
				context.write(new Text(currentGram.toString()), NullWritable.get());
				
				// dequeue a word so another word can be enqueued 
				wordQueue.poll();
			}
		}	
	}
}
