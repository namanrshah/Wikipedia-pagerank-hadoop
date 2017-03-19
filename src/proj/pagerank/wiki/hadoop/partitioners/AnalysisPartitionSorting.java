package proj.pagerank.wiki.hadoop.partitioners;

import proj.pagerank.wiki.hadoop.util.Constants;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AnalysisPartitionSorting extends Partitioner<FloatWritable, Text> {

    @Override
    public int getPartition(FloatWritable key, Text value, int numberOfReducers) {
        // TODO Auto-generated method stub

        if (value.toString().startsWith(Constants.IDEALIZED_PR)) {
            return 0;
        } else if (value.toString().startsWith(Constants.TAXATION_PR)) {
            return 1;
        } else {
            return 2;
        }
        // return characterList.indexOf(text.toString().charAt(0));
    }
}
