package calculatetfidf;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
 

public class WordsDocTFIDFMapper extends Mapper<LongWritable, Text, Text, Text> {
 
    public WordsDocTFIDFMapper() {
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] wordcounter = value.toString().split("\t");
        String[] word_doc = wordcounter[0].split("@");                 
        context.write(new Text(word_doc[0]), new Text(word_doc[1] + "=" + wordcounter[1]));
    }
}
