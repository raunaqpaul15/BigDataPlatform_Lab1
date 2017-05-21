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
 

public class WordFrequencyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	public WordFrequencyMapper() {
    }
	 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        // Compile all the words using regex
	        Pattern p = Pattern.compile("\\w+");
	        Matcher m = p.matcher(value.toString());
	 
	        // Get the name of the file from the input in the context
	        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	 
	        // build the values and write <k,v> pairs through the context
	        StringBuilder valueBuilder = new StringBuilder();
	        while (m.find()) {
	            String matchedKey = m.group().toLowerCase();
	            System.out.println(matchedKey);
	            // remove names starting with non letters, digits
	            if (!Character.isLetter(matchedKey.charAt(0)) || Character.isDigit(matchedKey.charAt(0))) {
	                continue;
	            }
	            valueBuilder.append(matchedKey);
	            valueBuilder.append("@");
	            valueBuilder.append(fileName);
	            context.write(new Text(valueBuilder.toString()), new IntWritable(1));
	            valueBuilder.setLength(0);
	        }
	    }
	}


