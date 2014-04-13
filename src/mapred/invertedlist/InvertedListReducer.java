package mapred.invertedlist;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class InvertedListReducer extends Reducer<Text, Text, Text, Text> {
	
	 /**
     * Accumulate the partial product sum into the product sum, similar with
     * word count reducer.
     */
    @Override
    protected void reduce(Text key, Iterable<Text> value, Context context)
            throws IOException, InterruptedException {
    	HashMap<String, Integer> vectorMap = new HashMap<String, Integer>();
    		
    	// get the vector map from the reducer input
        for (Text data : value) {
        	String url = data.toString();     	
        	// put new value into map
        	int newCounts = vectorMap.containsKey(url) ? vectorMap.get(url) + 1 : 1;
        	vectorMap.put(url,  newCounts);
        }

        
        // combine map data to form output inverted list vector
        StringBuilder builder = new StringBuilder();
        for (Entry<String, Integer> vectorElement : vectorMap.entrySet()) {
        	builder.append(vectorElement.getKey() + " " + vectorElement.getValue()+">>"); 
        }
        context.write(key, new Text(builder.toString()));
    }
}
