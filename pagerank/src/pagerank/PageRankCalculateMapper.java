package pagerank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import pagerank.PageRankDriver;



public class PageRankCalculateMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
     
        int index1 = value.find("\t");
        int index2 = value.find("\t", index1 + 1);
        
        // extract tokens from the current line
        String vertex = Text.decode(value.getBytes(), 0, index1);
        String pageRank = Text.decode(value.getBytes(), index1 + 1, index2 - (index1 + 1));
        String outlinks = Text.decode(value.getBytes(), index2 + 1, value.getLength() - (index2 + 1));
        
        String[] restVertices = outlinks.split(",");
        for (String othervertex : restVertices) { 
            Text pageRankTotal = new Text(pageRank + "\t" + restVertices.length);
            context.write(new Text(othervertex), pageRankTotal); 
        }
        
        context.write(new Text(vertex), new Text(PageRankDriver.LINKS_SEPARATOR + outlinks));
        
    }
    
}
