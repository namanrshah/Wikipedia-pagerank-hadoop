package proj.pagerank.wiki.hadoop.mappers;

import proj.pagerank.wiki.hadoop.util.Constants;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AnalysisMapperAvgDifference extends Mapper<LongWritable, Text, Text, FloatWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

//	
        try {

            String val = value.toString();
            if (!val.contains(Constants.DIFFERENCE_PR)) {
            } else {
                float diff = Float.parseFloat(val.split("\t")[1]);
                context.write(new Text(Constants.DIFFERENCE_PR), new FloatWritable(diff));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
