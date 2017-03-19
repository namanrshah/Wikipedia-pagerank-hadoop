package proj.pagerank.wiki.hadoop.reducers;

import proj.pagerank.wiki.hadoop.util.Constants;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AnalysisReducerWikiDownloaderPhase2 extends Reducer<Text, Text, Text, Text> {

    public NullWritable nullWritable = NullWritable.get();

    @Override
    protected void reduce(Text text, Iterable<Text> textTypes, Context context)
            throws IOException, InterruptedException {

        StringBuffer toWriteString = new StringBuffer();
//		int counter = 0;
//		int counter = text.toString();
        for (Text textToWrite : textTypes) {

            String toWrTemp = textToWrite.toString();
            if (!toWrTemp.isEmpty()) {
                toWriteString.append(toWrTemp);
            }

//			}
        }
        int occur = (toWriteString.length() - toWriteString.toString().replace(Constants.LINK_TO_LINK, "").length()) / 3;//length of LINK_TO_LINK
        if (toWriteString.length() != 0) {
            context.write(text, new Text(toWriteString.toString() + Constants.LINK_TO_OCCURANCE + occur + Constants.TITLE_TO_RANK + "1.0" + Constants.TITLE_TO_RANK + "1.0"));
        }
    }
}
