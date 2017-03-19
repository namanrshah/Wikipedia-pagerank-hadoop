package proj.pagerank.hadoop.mappers;

import proj.pagerank.hadoop.util.Constants;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AnalysisMapperWikiDownloadPhase1 extends
        Mapper<LongWritable, Text, Text, Text> {
    // public static int printData= 1;

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // if(printData %10 == 0)
        // {
        String xmlDump = value.toString();
        boolean isRedirected = xmlDump.contains("<redirect title");
        if (!isRedirected) {
            int startIndexTitle = xmlDump.indexOf("<title>");
            int endIndexTitle = xmlDump.indexOf("</title>");

            String pageTitle = xmlDump.substring(startIndexTitle + 7,
                    endIndexTitle);
            if (pageTitle.contains("/") && pageTitle.contains("Wikipedia:")) {
                pageTitle = pageTitle
                        .substring(0, pageTitle.indexOf("/"));
            }
            if (pageTitle.contains("#") && pageTitle.contains("Wikipedia:")) {
                pageTitle = pageTitle
                        .substring(0, pageTitle.indexOf("#"));
            }
            if (!pageTitle.isEmpty()) {
                context.write(new Text(pageTitle), new Text(Constants.DUMP_DOWNLOAD_TITLE));
            }
            String startingPage = xmlDump;
            boolean hasLinks = xmlDump.contains("[[");

            while (hasLinks) {
                try {
                    int startIndexLink = startingPage.indexOf("[[") + 2;
                    int endingIndexLink = startingPage.indexOf("]]");
                    String link = startingPage.substring(
                            startIndexLink, endingIndexLink);

                    boolean nestedLink = link.contains("[[");

                    if (nestedLink) {
                        // Ignoring Nested
                    } else {
                        if (link.contains("|")) {
                            link = link.substring(0,
                                    link.indexOf("|"));

                        }

                        if (link.contains("#") && link.contains("Wikipedia")) {
                            link = link.substring(0,
                                    link.indexOf("#"));
                        }
                        if (link.contains("/")
                                && link.contains("Wikipedia:")) {
                            link = link.substring(0,
                                    link.indexOf("/"));
                        }
                        if (!link.isEmpty()) {
                            context.write(new Text(link), new Text(
                                    pageTitle));
                        }

                    }
                    startingPage = startingPage
                            .substring(endingIndexLink + 2);
                    hasLinks = true;
                    if (!startingPage.contains("[[")) {
                        hasLinks = false;
                    }
                } catch (StringIndexOutOfBoundsException stringIndexOutOfBoundsException) {
                    System.err.println("Some Invalid String Detacted..");
                    hasLinks = false;
                }

            }

            // printData++;
        } else {
            //No redirection
            try {
                int startIndexTitle = xmlDump.indexOf("<title>");
                int endIndexTitle = xmlDump.indexOf("</title>");

                String pageTitle = xmlDump.substring(startIndexTitle + 7,
                        endIndexTitle);

                // String titleOfPage = valueRead.substring(startIndexOfTitle +
                // 7, endIndexOfTitle);
                if (pageTitle.contains("/")
                        && pageTitle.contains("Wikipedia:")) {
                    pageTitle = pageTitle.substring(0,
                            pageTitle.indexOf("/"));
                }
                if (pageTitle.contains("#")
                        && pageTitle.contains("Wikipedia:")) {
                    pageTitle = pageTitle.substring(0,
                            pageTitle.indexOf("#"));
                }
                int redirectStartingIndex = xmlDump.indexOf("<redirect title");
                int redirectEndingIndex = xmlDump.indexOf("/>") - 2;
                String redirectTitleTemp = xmlDump.substring(
                        redirectStartingIndex, redirectEndingIndex);
                int pageStart = "<redirect title=\"".length();
                int pageEnd = redirectTitleTemp.lastIndexOf("\"");
                String redirectTitle = redirectTitleTemp.substring(pageStart, pageEnd);
                if (!pageTitle.isEmpty() && !redirectTitle.isEmpty()) {
                    context.write(new Text(pageTitle), new Text(Constants.DUMP_DOWNLOAD_REDIRECT
                            + redirectTitle));
                }
            } catch (Exception e) {
                System.err.println("Some Invalid String Detacted..");
            }
        }
        // }
    }

}
