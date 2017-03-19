package proj.pagerank.wiki.hadoop.util;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class TemporaryReducer extends Reducer<Text, Text, Text, Text>
{
@Override
protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException,
		InterruptedException {
	// TODO Auto-generated method stub
	for(Text v : value)
		context.write(key, v);

}	

}
