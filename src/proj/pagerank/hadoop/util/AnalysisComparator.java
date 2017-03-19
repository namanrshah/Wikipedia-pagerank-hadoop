package proj.pagerank.hadoop.util;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AnalysisComparator extends WritableComparator {

	//Constructor.
    protected AnalysisComparator() {
        super(FloatWritable.class, true);
    }

//	@SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        FloatWritable f1 = (FloatWritable) w1;
        FloatWritable f2 = (FloatWritable) w2;

        return -1 * f1.compareTo(f2);
    }
}
