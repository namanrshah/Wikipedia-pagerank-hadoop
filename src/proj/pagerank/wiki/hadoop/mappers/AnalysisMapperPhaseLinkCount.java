package proj.pagerank.wiki.hadoop.mappers;

import proj.pagerank.wiki.hadoop.util.Constants;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AnalysisMapperPhaseLinkCount extends Mapper<LongWritable, Text, Text, NullWritable> {

    NullWritable nullWritable = NullWritable.get();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String val = value.toString();
//        try
//        {
        if (val.startsWith(Constants.TITLE_TO_ITERATE)) {
            val = val.substring(Constants.TITLE_TO_ITERATE.length());
        }
        if (!val.isEmpty()) {
            String[] titleToRanks = val.split(Constants.TITLE_TO_RANK);
//            Float pageRankIdealize = Float.parseFloat(firstSplit[1]);
//            Float pageRankTaxation = Float.parseFloat(firstSplit[2]);

            String[] linkToOccurance = titleToRanks[0].split(Constants.LINK_TO_OCCURANCE);
            int numberOfLinks = Integer.parseInt(linkToOccurance[1]);

            String[] linkToLink = linkToOccurance[0].split(Constants.LINK_TO_LINK);
            String title = linkToLink[0].trim();

//            Logger logger = Logger.getLogger(MatrixMulMapper.class);
//            for (int i = 0; i < split.length; i++) {
//                String split1 = split[i];
//                System.err.println("-split-" + split1);
//            }
//            Float sum = 0f;
            for (int i = 1; i <= numberOfLinks; i++) {
                context.write(new Text(linkToLink[i]), nullWritable);
            }
//            logger.info("");
            context.write(new Text(title), nullWritable);
        }
//        }
//        catch(Exception e)
//        {
//        	context.write(new Text("ErrorDetected For String: " + valueRead), new Text("Error: " + e.getMessage()));
//        }
//        context.write(new Text(key.toString()), value);
    }
}
