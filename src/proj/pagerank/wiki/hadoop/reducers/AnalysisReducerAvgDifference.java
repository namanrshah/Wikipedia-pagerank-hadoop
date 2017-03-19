package proj.pagerank.wiki.hadoop.reducers;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Reducer;

//import cs535.testing.CountMe;
public class AnalysisReducerAvgDifference extends Reducer<Text, FloatWritable, Text, NullWritable> {

    public NullWritable nullWritable = NullWritable.get();
    long divider;

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> floatTypes, Context context)
            throws IOException, InterruptedException {
        divider = Long.parseLong(context.getConfiguration().get("countOfLinks"));
        float sumForAvg = 0;

        for (FloatWritable f : floatTypes) {

            sumForAvg += f.get();

        }
        double avg = sumForAvg / divider;
        context.write(new Text("Average: " + avg), nullWritable);
//		}

    }

}
