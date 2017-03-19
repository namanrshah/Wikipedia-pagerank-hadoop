package proj.pagerank.hadoop.reducers;

import proj.pagerank.hadoop.util.Constants;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AnalysisReducerIteration extends Reducer<Text, Text, Text, NullWritable> {

    public static final float beta = (float) 0.85;
    public static final float oneMinusBeta = (float) (1 - 0.85);
    public NullWritable n = NullWritable.get();
    // DecimalFormat df = new DecimalFormat("#.##");
    public String stringGot = "";

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        StringBuffer toWriteString = new StringBuffer();
//		int length = 0;
        double pageRankIdealize = 0.0;
        double pageRankTaxed = 0.0;
//		int totalTitles = 5000000;
        boolean isTitle = false;
        for (Text value : values) {
            stringGot = value.toString();
            if (!stringGot.startsWith(Constants.TITLE_TO_ITERATE)) {
                // stringGot = (value.toString());
                String[] split = stringGot.split(Constants.VALUE_SEPARATOR);
                int share = Integer.parseInt(split[0]);
                // totalTitles = Integer.parseInt(split[2]);
                double previousPRIdealize = Float.parseFloat(split[1]);
                pageRankIdealize += previousPRIdealize / share;
                double previousPRTaxed = Float.parseFloat(split[2]);
                pageRankTaxed += (previousPRTaxed) / share;
            } else {
//				context.write(new Text("Title Recieved: " + stringGot + ""), n);
                toWriteString.append(stringGot);
                isTitle = true;
            }

        }
        if (!isTitle) {
            // Dead End Logic
            toWriteString.append(Constants.TITLE_TO_ITERATE).append(key.toString()).append(Constants.LINK_TO_LINK).append("No_Link").append(Constants.LINK_TO_OCCURANCE).append("0").append(Constants.TITLE_TO_RANK);

        }
        //
        pageRankTaxed = ((pageRankTaxed * beta) + (oneMinusBeta));
        // toWrite.append();
        toWriteString.append(pageRankIdealize).append(Constants.TITLE_TO_RANK).append(pageRankTaxed);
        context.write(new Text(toWriteString.toString()), n);
//		}
    }
}
