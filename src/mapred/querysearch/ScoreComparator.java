package mapred.querysearch;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ScoreComparator extends WritableComparator {
    protected ScoreComparator() {
        super(DoubleWritable.class, true);
    }

    // use reverse order
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        return -super.compare(w1,w2); 
    }
}
