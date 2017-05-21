package calculatetfidf;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordsDocTFIDFReducer extends Reducer<Text, Text, Text, Text> {
 
    private static final DecimalFormat DF = new DecimalFormat("###.#####");
 
    public WordsDocTFIDFReducer() {
    }

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        int num_docs = Integer.parseInt(context.getJobName());
       
        int num_doc_key = 0;
        Map<String, String> temp_freq = new HashMap<String, String>();
        for (Text val : values) {
            String[] doc_freq = val.toString().split("=");
            num_doc_key++;
            temp_freq.put(doc_freq[0], doc_freq[1]);
        }
        for (String doc : temp_freq.keySet()) {
            String[] word_freq_total = temp_freq.get(doc).split("/");
 
            
            double tf = Double.valueOf(Double.valueOf(word_freq_total[0]) / Double.valueOf(word_freq_total[1]));
            double idf = (double) num_docs / (double) num_doc_key;
  
            double tfIdf = num_docs == num_doc_key ? tf : tf * Math.log(idf);
 
            context.write(new Text(key + "@" + doc), new Text("       " + DF.format(tfIdf) + " "));
        }
    }
}
