package csvcontentyearheight;

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;



public class CsvContentRead {

	public static void main(String[] args) throws IOException {
		
		
		Path filename = new Path("csvinput/arbres.csv");
		
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream inputStream = fs.open(filename);
	    String output="/home/cloudera/workspace/csvcontentyearheight/displayoutput.txt";
	    FSDataOutputStream output_path=fs.create(new Path(output));
		try{
			InputStreamReader isreader = new InputStreamReader(inputStream);
			BufferedReader br = new BufferedReader(isreader);
				
			String line = br.readLine();
			while (line !=null){
				
				String[] row = line.split(";");
				//System.out.println(row[5]+" "+row[6]);
				String s=row[5] + "\t" + row[6] + "\n";
				output_path.writeBytes(s);
				line = br.readLine();
			}
		}
		finally{
			inputStream.close();
			fs.close();
			output_path.close();
		}
		
	}

}