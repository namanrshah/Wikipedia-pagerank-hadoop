package proj.pagerank.hadoop.reducers;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Reducer;

//import cs535.testing.CountMe;
public class AnalysisReducerSorting extends Reducer<FloatWritable, Text, Text, FloatWritable> {

    public NullWritable nullWritable = NullWritable.get();
    long toDivide;

    @Override
    protected void reduce(FloatWritable prValue, Iterable<Text> textTypes, Context context)
            throws IOException, InterruptedException {

        toDivide = Long.parseLong(context.getConfiguration().get("countOfLinks"));

        for (Text titles : textTypes) {
            float floatWrite = prValue.get() / toDivide;
            context.write(new Text(titles.toString()), new FloatWritable(floatWrite));
        }

    }

}
