package mapred.ngramcount;

import java.io.IOException;

import mapred.job.Optimizedjob;
import mapred.util.SimpleParser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class Driver {

	public static void main(String args[]) throws Exception {
		SimpleParser parser = new SimpleParser(args);

		String input = parser.get("input");
		String output = parser.get("output");
		String gramCountArg = parser.get("n");
		
		int gramCount;
		try {
		    gramCount = Integer.parseInt(gramCountArg);
		} catch (Exception e){
		    System.out.println("invalid value for n - NAN");
		    gramCount = 1;
		}
		
		if(gramCount < 1){
			System.out.println(gramCount + " is an invalid value for n");
			gramCount = 1;
		}

		getJobFeatureVector(input, output, gramCount);

	}

	private static void getJobFeatureVector(String input, String output, int gramCount)
			throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration jobConf = new Configuration();
		
		jobConf.setInt("n", gramCount);
		
		Optimizedjob job = new Optimizedjob(jobConf, input, output,
				"Compute NGram Count");

		job.setClasses(NgramCountMapper.class, NgramCountReducer.class, null);
		job.setMapOutputClasses(Text.class, NullWritable.class);

		job.run();
	}	
}
