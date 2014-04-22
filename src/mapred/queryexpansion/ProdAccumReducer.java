package mapred.queryexpansion;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProdAccumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

	/**
	 * Accumulte dot products for words
     */
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> value, Context context)
            throws IOException, InterruptedException {
    	
    	double total = 0.0;
    	
    	// sum the partial dot products
    	for(DoubleWritable writeableVal : value){
    		
    		double partialVal = writeableVal.get();
    		
    		total += partialVal;
    	}
    	
    	// output the totaled value
    	context.write(key, new DoubleWritable(total));
    	
    }
	
}
