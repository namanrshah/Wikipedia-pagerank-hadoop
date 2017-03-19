package proj.pagerank.wiki.hadoop.partitioners;

import java.util.Random;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AnalysisPartitionIteration extends Partitioner<Text, Text> {

    Random r = new Random();

    @Override
    public int getPartition(Text key, Text value, int numberOfReducers) {
        // TODO Auto-generated method stub

        if (key.toString().startsWith("ErrorDetected")) {
            return 40;
        } else {
            return (r.nextInt(40));
        }
        // return characterList.indexOf(text.toString().charAt(0));
    }

}
