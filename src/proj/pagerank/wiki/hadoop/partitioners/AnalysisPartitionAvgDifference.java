package proj.pagerank.wiki.hadoop.partitioners;

import proj.pagerank.wiki.hadoop.util.Constants;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AnalysisPartitionAvgDifference extends Partitioner<Text, FloatWritable> {

    @Override
    public int getPartition(Text key, FloatWritable value, int numberOfReducers) {

        if (key.toString().startsWith(Constants.IDEALIZED_PR)) {
            return 0;
        } else if (key.toString().startsWith(Constants.TAXATION_PR)) {
            return 1;
        } else {
            return 2;
        }
        // return characterList.indexOf(text.toString().charAt(0));
    }
}
