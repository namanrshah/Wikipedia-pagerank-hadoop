package proj.pagerank.hadoop.mappers;

import proj.pagerank.hadoop.util.Constants;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AnalysisMapperWikiDownloaderPhase2 extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {

            String[] titleToLink = value.toString().split(Constants.TITLE_TO_LINK);
            String title = titleToLink[0].trim();
            String link = titleToLink[1].trim();
            if (!title.isEmpty() && !link.isEmpty()) {
                context.write(new Text(title), new Text(Constants.LINK_TO_LINK + link));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
