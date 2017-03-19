package proj.pagerank.hadoop.util;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AnalysisCombinerWikiDownloadPhase2 extends Reducer<Text, Text, Text, Text> {

//	public NullWritable nullWritable = NullWritable.get();
    // public String toWriteDown = "";
    // public int count = 0;
    @Override
    protected void reduce(Text text, Iterable<Text> textTypes, Context context)
            throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();
        for (Text toWrite : textTypes) {
            sb.append(toWrite.toString());
        }
        context.write(text, new Text(sb.toString()));

    }
}
