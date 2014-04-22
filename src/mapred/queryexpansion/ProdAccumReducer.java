package mapred.queryexpansion;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ProdAccumReducer extends Reducer<Text, VIntWritable, Text, VIntWritable>{

	/**
	 * Accumulte dot products for words
     */
    @Override
    protected void reduce(Text key, Iterable<VIntWritable> value, Context context)
            throws IOException, InterruptedException {
    	
    	int total = 0;
    	
    	// sum the partial dot products
    	for(VIntWritable writeableVal : value){
    		int partialVal = writeableVal.get(); 		
    		total += partialVal;
    	}
    	
    	// output the totaled value
    	context.write(key, new VIntWritable(total));
    	
    }
	
}
