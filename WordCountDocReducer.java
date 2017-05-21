package calculatetfidf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountDocReducer extends Reducer<Text, Text, Text, Text> {
 
    public WordCountDocReducer() {
    }

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sum_words = 0;
        Map<String, Integer> tempcounter = new HashMap<String, Integer>();
        for (Text val : values) {
            String[] wordcounter = val.toString().split("=");
            tempcounter.put(wordcounter[0], Integer.valueOf(wordcounter[1]));
            sum_words += Integer.parseInt(val.toString().split("=")[1]);
        }
        for (String wordKey : tempcounter.keySet()) {
            context.write(new Text(wordKey + "@" + key.toString()), new Text(tempcounter.get(wordKey) + "/"
                    + sum_words));
        }
    }
}