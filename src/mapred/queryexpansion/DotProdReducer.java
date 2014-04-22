package mapred.queryexpansion;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class DotProdReducer extends Reducer<Text, Text, Text, VIntWritable>{
	

	/**
	 * Compute dot product pairs for all words, based on sites
     */
    @Override
    protected void reduce(Text key, Iterable<Text> value, Context context)
            throws IOException, InterruptedException {

    	ArrayList<Integer> appears = new ArrayList<Integer>();
    	ArrayList<String> words = new ArrayList<String>();
    	
    	Text output_key = new Text();
    	VIntWritable output_value = new VIntWritable();
    	
    	// get the appears and words from iterable value
    	for (Text pair : value) {
    		String [] factorArrA = pair.toString().split(" ");
        	
    		words.add(factorArrA[0]);
        	appears.add(Integer.parseInt(factorArrA[1]));
    	}
    	
    	// for each value, iterate through all values and compute products
        for (int i = 0; i < appears.size()-1; i++) { 	
        	for(int j = i+1; j < appears.size(); j++){
            	String wordA = words.get(i);
            	String wordB = words.get(j);
            	
            	
            	StringBuilder sb = new StringBuilder();
            	// words need to be lexigraphically ordered so all products can be properly accumulated
            	if(wordA.compareTo(wordB) > 0){		
            		sb.append(wordA);
            		sb.append(" ");
            		sb.append(wordB);
            	}else{          		
            		sb.append(wordB);
            		sb.append(" ");
            		sb.append(wordA);
            	}
            	
            	int valueA = appears.get(i);
            	int valueB = appears.get(j);
            	int product = valueA*valueB;
            	
            	output_key.set(sb.toString());
            	output_value.set(product);
            	// output the dot product
            	context.write(output_key, output_value);	
        	}
        }
    }
}
