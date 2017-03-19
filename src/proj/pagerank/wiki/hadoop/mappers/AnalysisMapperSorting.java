package proj.pagerank.wiki.hadoop.mappers;

import proj.pagerank.wiki.hadoop.util.Constants;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class AnalysisMapperSorting extends
        Mapper<LongWritable, Text, FloatWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String val = value.toString();
        // try
        // {
        if (val.startsWith(Constants.TITLE_TO_ITERATE)) {
            val = val.substring(Constants.TITLE_TO_ITERATE.length());
        }
        if (!val.isEmpty()) {
            String[] titleToRank = val.split(Constants.TITLE_TO_RANK);
            Float pageRankIdealize = Float.parseFloat(titleToRank[1]);
            Float pageRankTaxation = Float.parseFloat(titleToRank[2]);

            String[] linkToLink = titleToRank[0].split(Constants.LINK_TO_LINK);
            String title = linkToLink[0].trim();

            context.write(new FloatWritable(pageRankIdealize), new Text(
                    Constants.IDEALIZED_PR + title));
            context.write(new FloatWritable(pageRankTaxation), new Text(
                    Constants.TAXATION_PR + title));

            context.write(new FloatWritable((pageRankTaxation - pageRankIdealize)), new Text(
                    Constants.DIFFERENCE_PR + title));

        }

    }

}
