package mapred.queryexpansion;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DotProdReducer extends Reducer<Text, Text, Text, DoubleWritable>{

	/**
	 * Compute dot product pairs for all words, based on sites
     */
    @Override
    protected void reduce(Text key, Iterable<Text> value, Context context)
            throws IOException, InterruptedException {

    	HashSet<String> computedProducts = new HashSet<String>();
    	
    	// for each value, iterate through all values and compute products
        for (Text prodFactorA : value) {
        	
        	String [] factorArrA = value.toString().split(" ");
        	String wordA = factorArrA[0];
        	String valueAstr = factorArrA[1];
        	
        	for(Text prodFactorB : value){
        		
        		if(prodFactorB.toString().equals(prodFactorA.toString())) continue;
        		
            	String [] factorArrB = value.toString().split(" ");
            	String wordB = factorArrB[0];
            	String valueBstr = factorArrB[1];
            	
            	// all products for this word were already computed, nothing to do ehre
            	if(computedProducts.contains(wordB)) continue;
            	
            	StringBuilder sb = new StringBuilder();
            	
            	// words need to be lexigraphically ordered so all products can be properly accumulated
            	if(wordA.compareTo(wordB) > 0){
            		
            		sb.append(wordA);
            		sb.append(wordB);
            	}else{
            		
            		sb.append(wordB);
            		sb.append(wordA);
            	}
            	
            	String output_key = sb.toString();
            	
            	int valueA = Integer.parseInt(valueAstr);
            	int valueB = Integer.parseInt(valueBstr);
            	double product = valueA*valueB;
            	
            	// output the dot product
            	context.write(new Text(sb.toString()), new DoubleWritable(product));	
        	}
        	
        	//all product pairs for this word computed, any products containing it should not be revisited
        	computedProducts.add(wordA);
        }
    }
}
