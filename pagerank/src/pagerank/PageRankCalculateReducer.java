package pagerank;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import pagerank.PageRankDriver;

public class PageRankCalculateReducer extends Reducer<Text, Text, Text, Text> {
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, 
                                                                                InterruptedException {

        String links = "";
        double sumRestPageRanks = 0.0;
        
        for (Text value : values) {
 
            String content = value.toString();
            
            if (content.startsWith(PageRankDriver.LINKS_SEPARATOR)) {
             
                links += content.substring(PageRankDriver.LINKS_SEPARATOR.length());
            } else {
                
                String[] split = content.split("\\t");
              
                double pageRank = Double.parseDouble(split[0]);
                int totalLinks = Integer.parseInt(split[1]);
                
                sumRestPageRanks += (pageRank / totalLinks);
            }

        }
        
        double newRank = PageRankDriver.DAMPING * sumRestPageRanks + (1 - PageRankDriver.DAMPING);
        context.write(key, new Text(newRank + "\t" + links));
        
    }

}