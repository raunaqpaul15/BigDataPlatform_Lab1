package calculatetfidf;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
 

public class TFIDFDriver extends Configured implements Tool {
 
    // where to put the data in hdfs 
    private static final String OUTPUT_PATH = "tfidfoutput";
 
    // where to read the data from.
    private static final String INPUT_PATH = "output2";
 
    public int run(String[] args) throws Exception {
 
        Configuration conf = getConf();
        Job job = new Job(conf, "TF-IDF");
 
        job.setJarByClass(TFIDFDriver.class);
        job.setMapperClass(WordsDocTFIDFMapper.class);
        job.setReducerClass(WordsDocTFIDFReducer.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
 
        Path inputPath = new Path("input");
        FileSystem fs = inputPath.getFileSystem(conf);
        FileStatus[] status = fs.listStatus(inputPath);
 
        job.setJobName(String.valueOf(status.length));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
 
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TFIDFDriver(), args);
        System.exit(res);
    }
}