package proj.pagerank.hadoop.job;

import proj.pagerank.hadoop.reducers.AnalysisReducerPhaseLinkCount;
import proj.pagerank.hadoop.partitioners.AnalysisPartitionSorting;
import proj.pagerank.hadoop.mappers.AnalysisMapperPhaseLinkCount;
import proj.pagerank.hadoop.reducers.AnalysisReducerIteration;
import proj.pagerank.hadoop.reducers.AnalysisReducerAvgDifference;
import proj.pagerank.hadoop.reducers.AnalysisReducerWikiDownloaderPhase2;
import proj.pagerank.hadoop.reducers.AnalysisReducerSorting;
import proj.pagerank.hadoop.reducers.AnalysisReducerWikiDownloadPhase1;
import proj.pagerank.hadoop.mappers.AnalysisMapperAvgDifference;
import proj.pagerank.hadoop.mappers.AnalysisMapperWikiDownloaderPhase2;
import proj.pagerank.hadoop.mappers.AnalysisMapperSorting;
import proj.pagerank.hadoop.mappers.AnalysisMapperIteration;
import proj.pagerank.hadoop.mappers.AnalysisMapperWikiDownloadPhase1;
import proj.pagerank.hadoop.util.AnalysisCombinerWikiDownloadPhase2;
import proj.pagerank.hadoop.util.AnalysisComparator;
import proj.pagerank.hadoop.util.CounterEnum;
import proj.pagerank.hadoop.util.XmlInputFormat;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//import cs535.testing.CounterEnum;
public class AnalysisJob {

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");
        // //
        // // // Phase 1 --- Getting Title To Links...
        Job wikiDumpDownload1 = Job.getInstance(conf, "WikiDumpDownload1");
        wikiDumpDownload1.setJarByClass(AnalysisJob.class);
        // // //
        wikiDumpDownload1.setInputFormatClass(XmlInputFormat.class);
        // // //
        wikiDumpDownload1.setMapperClass(AnalysisMapperWikiDownloadPhase1.class);
        wikiDumpDownload1.setReducerClass(AnalysisReducerWikiDownloadPhase1.class);
        // // //
        wikiDumpDownload1.setNumReduceTasks(40);
        wikiDumpDownload1.setMapOutputKeyClass(Text.class);
        wikiDumpDownload1.setMapOutputValueClass(Text.class);
        //
        wikiDumpDownload1.setOutputKeyClass(Text.class);
        wikiDumpDownload1.setOutputValueClass(NullWritable.class);
        //
        FileInputFormat.addInputPath(wikiDumpDownload1, new Path(args[0]));
        FileSystem fileSystem = FileSystem.get(conf);
//        //
        String outputPath = args[1] + "_Phase1";
        // // //
        if (fileSystem.exists(new Path(outputPath))) {
            fileSystem.delete(new Path(outputPath), true);
        }
        //
        FileOutputFormat.setOutputPath(wikiDumpDownload1, new Path(outputPath));
        wikiDumpDownload1.waitForCompletion(true);
        //
        // //
        // //
        // //
        // // //Phase 2 ----- Removing Unwanted Links
        // //
        //
        Job wikiDumDownload2 = Job.getInstance(conf, "WikiDumpDownload2");
        // //
        // //
        // //
        wikiDumDownload2.setJarByClass(AnalysisJob.class);
        // //
        // //
        wikiDumDownload2.setMapperClass(AnalysisMapperWikiDownloaderPhase2.class);
        wikiDumDownload2.setCombinerClass(AnalysisCombinerWikiDownloadPhase2.class);
        // // ////// job.setPartitionerClass(AnalysisPartition.class);
        // // // //////
        wikiDumDownload2.setReducerClass(AnalysisReducerWikiDownloaderPhase2.class);
        // //
        wikiDumDownload2.setNumReduceTasks(40);
        // //
        // //
        // String outputPath = args[0];
        wikiDumDownload2.setMapOutputKeyClass(Text.class);
        wikiDumDownload2.setMapOutputValueClass(Text.class);
        // //
        wikiDumDownload2.setOutputKeyClass(Text.class);
        wikiDumDownload2.setOutputValueClass(NullWritable.class);
        // // FileSystem fileSystem = FileSystem.get(conf);
        FileInputFormat.addInputPath(wikiDumDownload2, new Path(outputPath));
        // // //
        // // //
        String outputPathPhase2 = args[1] + "_Phase2";
        // //
        if (fileSystem.exists(new Path(outputPathPhase2))) {
            fileSystem.delete(new Path(outputPathPhase2), true);
        }
        // //
        FileOutputFormat.setOutputPath(wikiDumDownload2, new Path(outputPathPhase2));
        wikiDumDownload2.waitForCompletion(true);

        fileSystem.delete(new Path(outputPath), true);

        //
        //
        //
        String inputToCount = outputPathPhase2;
//        String inputToCount = args[0];

        Job titleCount = Job.getInstance(conf, "WikiTitleCount");
        // //
        // //
        // //
        titleCount.setJarByClass(AnalysisJob.class);
        // //
        // job.setInputFormatClass(XmlInputFormat.class);
        // //
        titleCount.setMapperClass(AnalysisMapperPhaseLinkCount.class);
        // // //
        // // // job.setPartitionerClass(AnalysisPartition.class);
        // // //
        titleCount.setReducerClass(AnalysisReducerPhaseLinkCount.class);
        // //
        titleCount.setNumReduceTasks(40);
        // //
        // //
        // //
        titleCount.setMapOutputKeyClass(Text.class);
        titleCount.setMapOutputValueClass(NullWritable.class);
        //
        titleCount.setOutputKeyClass(Text.class);
        titleCount.setOutputValueClass(NullWritable.class);
        //
        FileInputFormat.addInputPath(titleCount, new Path(inputToCount));
        // FileSystem fileSystem = FileSystem.get(conf);

        String outputPathToCount = args[1] + "_Count";
        //
        if (fileSystem.exists(new Path(outputPathToCount))) {
            fileSystem.delete(new Path(outputPathToCount), true);
        }

        FileOutputFormat.setOutputPath(titleCount, new Path(outputPathToCount));
        titleCount.waitForCompletion(true);

        Counters counters = titleCount.getCounters();

        // counters.
//         long counter = counters.findCounter("Map-Reduce Framework",
//         "Map output records").getValue();
        System.out.println("Total Titles Found: "
                + new Long(counters.findCounter(CounterEnum.titleCounts)
                        .getValue()));
        fileSystem.delete(new Path(outputPathToCount), true);
        // String inputToCount = outputPathPhase2;

//         String inputPathIteration = args[1] + "_6";
//        String inputPathIteration = args[0];
        String inputPathIteration = outputPathPhase2;
        for (int i = 1; i <= 25; i++) {

            Job iteration = Job.getInstance(conf, "PagerankIteration" + i);

            iteration.setJarByClass(AnalysisJob.class);

            // job.setInputFormatClass(TextInputFormat.class);
            iteration.setMapperClass(AnalysisMapperIteration.class);
            // jobNew.setPartitionerClass(AnalysisPartitionPhase3.class);
            //
            // job.setPartitionerClass(AnalysisPartition.class);
            //
            iteration.setReducerClass(AnalysisReducerIteration.class);

            iteration.setNumReduceTasks(40);
            iteration.setMapOutputKeyClass(Text.class);
            iteration.setMapOutputValueClass(Text.class);

            iteration.setOutputKeyClass(Text.class);
            iteration.setOutputValueClass(NullWritable.class);
            String outputPathIteration = args[1] + "_" + i;
            // job.addCacheFile(new URI("hdfs://raleigh:32201" + args[1] +
            // "#Temp"));
            FileInputFormat.addInputPath(iteration, new Path(inputPathIteration));

            if (fileSystem.exists(new Path(outputPathIteration))) {
                fileSystem.delete(new Path(outputPathIteration), true);
            }

            FileOutputFormat.setOutputPath(iteration,
                    new Path(outputPathIteration));
            iteration.waitForCompletion(true);

            if (i >= 2) {
                fileSystem.delete(new Path(args[1] + "_" + (i - 1)), true);
            }
            inputPathIteration = outputPathIteration;
        }

        // Counters counter = jobToCount.getCounters();
        // counters.
        // long counter = counters.findCounter("Map-Reduce Framework",
        // "Map output records").getValue();
        System.out.println("Total Links Found: "
                + new Long(counters.findCounter(CounterEnum.titleCounts)
                        .getValue()));

        // Descending
        Configuration confNew = new Configuration();
        confNew.set("countOfLinks", String.valueOf((counters
                .findCounter(CounterEnum.titleCounts).getValue())));

        Job pagerankSorting = Job.getInstance(confNew, "PagerankSorting");
        pagerankSorting.setJarByClass(AnalysisJob.class);
        pagerankSorting.setMapperClass(AnalysisMapperSorting.class);

        pagerankSorting.setSortComparatorClass(AnalysisComparator.class);
        pagerankSorting.setPartitionerClass(AnalysisPartitionSorting.class);
        pagerankSorting.setReducerClass(AnalysisReducerSorting.class);

        pagerankSorting.setNumReduceTasks(3);
        pagerankSorting.setMapOutputKeyClass(FloatWritable.class);
        pagerankSorting.setMapOutputValueClass(Text.class);

        pagerankSorting.setOutputKeyClass(Text.class);
        pagerankSorting.setOutputValueClass(FloatWritable.class);
        String outputPathSorted = args[1] + "_Sorted";
        FileInputFormat.addInputPath(pagerankSorting, new Path(inputPathIteration));

        if (fileSystem.exists(new Path(outputPathSorted))) {
            fileSystem.delete(new Path(outputPathSorted), true);
        }

        FileOutputFormat.setOutputPath(pagerankSorting, new Path(outputPathSorted));
        pagerankSorting.waitForCompletion(true);

//        confNew.set("countOfLinks", String.valueOf(7107930));

        Job averageRanks = Job.getInstance(confNew, "average_ranks");

        averageRanks.setJarByClass(AnalysisJob.class);

        // job.setInputFormatClass(TextInputFormat.class);
        averageRanks.setMapperClass(AnalysisMapperAvgDifference.class);
        averageRanks.setReducerClass(AnalysisReducerAvgDifference.class);

//		jobPhase5.setNumReduceTasks(3);
        averageRanks.setMapOutputKeyClass(Text.class);
        averageRanks.setMapOutputValueClass(FloatWritable.class);

        averageRanks.setOutputKeyClass(Text.class);
        averageRanks.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(averageRanks, new Path(outputPathSorted));
        String outputPathFinal = args[1] + "_Average_ranks";

//		FileSystem fileSystem = FileSystem.get(confNew);
        if (fileSystem.exists(new Path(outputPathFinal))) {
            fileSystem.delete(new Path(outputPathFinal), true);
        }

        FileOutputFormat.setOutputPath(averageRanks, new Path(outputPathFinal));
        averageRanks.waitForCompletion(true);
        System.out.println("INFO : Job Completed..");

    }
}
