package pagerank;


import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import pagerank.PageRankCalculateMapper;
import pagerank.PageRankCalculateReducer;
import pagerank.VertexMapper;
import pagerank.VertexReducer;
import pagerank.PageRankSort;

public class PageRankDriver{
    

    

    public static NumberFormat NF = new DecimalFormat("00");
    public static Set<String> LIST = new HashSet<String>();
    public static String LINKS_SEPARATOR = "@";
    public static Double DAMPING = 0.85;
    public static int ITERATIONS = 2;
    public static String IN_PATH = "prinput";
    public static String OUT_PATH = "proutput";
    
    public static void main(String[] args) throws Exception {

        
        String inPath = null;;
        String lastOutPath = null;
        PageRankDriver pagerank = new PageRankDriver();
        
        System.out.println("Running Job 1 ");
        boolean isCompleted = pagerank.job1(IN_PATH, OUT_PATH + "/00");
        if (!isCompleted) {
            System.exit(1);
        }
        
        for (int runs = 0; runs < ITERATIONS; runs++) {
            inPath = OUT_PATH + "/" + NF.format(runs);
            lastOutPath = OUT_PATH + "/" + NF.format(runs + 1);
            System.out.println("Running Job 2 [" + (runs + 1) + "/" + PageRankDriver.ITERATIONS + "] ");
            isCompleted = pagerank.job2(inPath, lastOutPath);
            if (!isCompleted) {
                System.exit(1);
            }
        }
        
        System.out.println("Running Job 3");
        isCompleted = pagerank.job3(lastOutPath, OUT_PATH + "/result");
        if (!isCompleted) {
            System.exit(1);
        }
        

        System.exit(0);
    }
    
    public boolean job1(String in, String out) throws IOException,ClassNotFoundException, 
                                                      InterruptedException {
        
        Job job = Job.getInstance(new Configuration(), "Vertex Mapper");
        job.setJarByClass(PageRankDriver.class);
        
     
        FileInputFormat.addInputPath(job, new Path(in));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(VertexMapper.class);
        

        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(VertexReducer.class);
        
        return job.waitForCompletion(true);
     
    }
    

    public boolean job2(String in, String out) throws IOException, ClassNotFoundException, 
                                                      InterruptedException {
        
        Job job = Job.getInstance(new Configuration(), "Pagerank Calculation");
        job.setJarByClass(PageRankDriver.class);

        FileInputFormat.setInputPaths(job, new Path(in));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(PageRankCalculateMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(PageRankCalculateReducer.class);

        return job.waitForCompletion(true);
        
    }
    
    public boolean job3(String in, String out) throws IOException, 
    ClassNotFoundException, 
    InterruptedException {

Job job = Job.getInstance(new Configuration(), "Job 3");
job.setJarByClass(PageRankDriver.class);


FileInputFormat.setInputPaths(job, new Path(in));
job.setInputFormatClass(TextInputFormat.class);
job.setMapOutputKeyClass(DoubleWritable.class);
job.setMapOutputValueClass(Text.class);
job.setMapperClass(PageRankSort.class);

FileOutputFormat.setOutputPath(job, new Path(out));
job.setOutputFormatClass(TextOutputFormat.class);
job.setOutputKeyClass(DoubleWritable.class);
job.setOutputValueClass(Text.class);

return job.waitForCompletion(true);

}

    
}