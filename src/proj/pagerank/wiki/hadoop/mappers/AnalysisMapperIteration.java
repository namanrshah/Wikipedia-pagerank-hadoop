package proj.pagerank.wiki.hadoop.mappers;

import proj.pagerank.wiki.hadoop.util.Constants;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AnalysisMapperIteration extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String val = value.toString();
//        try
//        {

        if (val.startsWith(Constants.TITLE_TO_ITERATE)) {
            val = val.substring(Constants.TITLE_TO_ITERATE.length());
        }
        if (!val.isEmpty()) {
            String[] titleToRank = val.split(Constants.TITLE_TO_RANK);
            Float pageRankIdealize = Float.parseFloat(titleToRank[1]);
            Float pageRankTaxation = Float.parseFloat(titleToRank[2]);

            String[] linkToOccurance = titleToRank[0].split(Constants.LINK_TO_OCCURANCE);
            int numberOfLinks = Integer.parseInt(linkToOccurance[1]);

            String[] linkTolink = linkToOccurance[0].split(Constants.LINK_TO_LINK);
            String title = linkTolink[0].trim();
            for (int i = 1; i <= numberOfLinks; i++) {
                context.write(new Text(linkTolink[i]), new Text(numberOfLinks + Constants.VALUE_SEPARATOR + pageRankIdealize + Constants.VALUE_SEPARATOR + pageRankTaxation));
            }
//            logger.info("");
            context.write(new Text(title), new Text(Constants.TITLE_TO_ITERATE + titleToRank[0] + Constants.TITLE_TO_RANK));
        }
    }
}
