package proj.pagerank.hadoop.reducers;

import proj.pagerank.hadoop.util.Constants;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AnalysisReducerWikiDownloadPhase1 extends Reducer<Text, Text, Text, NullWritable> {

    NullWritable nullWritable = NullWritable.get();
    int redirectLength = Constants.DUMP_DOWNLOAD_REDIRECT.length();

    @Override
    protected void reduce(Text text, Iterable<Text> textTypes, Context context) throws IOException, InterruptedException {
        Set<String> uniqueLinks = new HashSet<String>();
        String originalTitle = "";
        for (Text textToWrite : textTypes) {

            String dataString = textToWrite.toString();
            if (dataString.contains(Constants.DUMP_DOWNLOAD_TITLE)) {
                originalTitle = text.toString();
            } else if (dataString.contains(Constants.DUMP_DOWNLOAD_REDIRECT)) {

                dataString = dataString.substring(redirectLength);
                // uniqueLinks.add(dataString);
                originalTitle = dataString;
            } // originalTitle = dataString;
            // }
            else {
                if (dataString.length() != 0) {
                    uniqueLinks.add(dataString);
                }
            }

        }
		// float numberOfLinks = uniqueLinks.size();
        // float numberAssigned = (float) (1.0 / numberOfLinks);
        if (!originalTitle.equals("")) {

            for (String uniqueLink : uniqueLinks) {

                context.write(new Text(uniqueLink + Constants.TITLE_TO_LINK + originalTitle), NullWritable.get());
            }
        }
    }
}
