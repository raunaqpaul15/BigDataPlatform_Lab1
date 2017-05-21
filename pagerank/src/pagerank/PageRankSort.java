package pagerank;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import pagerank.PageRankDriver;

public class PageRankSort extends Mapper<LongWritable, Text,DoubleWritable, Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
       
        int index1 = value.find("\t");
        int index2 = value.find("\t", index1 + 1);
        
        // extract tokens from the current line
        String vertex = Text.decode(value.getBytes(), 0, index1);
        float rank = Float.parseFloat(Text.decode(value.getBytes(),index1 + 1,index2 - (index1 + 1)));
        
        context.write(new DoubleWritable(rank),new Text(vertex));
        
    }
       
}