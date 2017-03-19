package proj.pagerank.hadoop.reducers;

import proj.pagerank.hadoop.util.CounterEnum;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AnalysisReducerPhaseLinkCount extends Reducer<Text, NullWritable, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {
        context.getCounter(CounterEnum.titleCounts).increment(1L);	
    }
}
