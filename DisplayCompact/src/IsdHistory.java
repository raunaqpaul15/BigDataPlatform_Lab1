
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;



public class IsdHistory {

	public static void main(String[] args) throws IOException {
		
		
		Path filename = new Path("input/isd-history.txt");
		
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream inputStream = fs.open(filename);
		String output="dispIsdHistory.txt";
	    FSDataOutputStream output_path=fs.create(new Path(output));
		try{
			
			InputStreamReader isreader = new InputStreamReader(inputStream);
			BufferedReader br = new BufferedReader(isreader);

			String line = br.readLine();
			int count = 1;
			String header="USAF \t StationName        \t FIPS \tAltitude \n";
			while (line !=null){
				if(count == 1) { 
					output_path.writeBytes(header);
				}
				else
				if (count > 22){
			
				String usaf = line.substring(0, 6).trim();
				String sname = line.substring(13, 13+29).trim();
				String fips = line.substring(43, 43+2).trim();
				String alt = line.substring(74, 74+7).trim();
				
				String row = usaf + " \t " + sname + "         \t "+ fips + " \t" + alt + "\n";
				output_path.writeBytes(row);

				
				// go to the next line
				
				}
				line = br.readLine();
				count +=1;
				
			}
		}
		finally{
			//close the file
			
			inputStream.close();
			fs.close();
			output_path.close();
		}

		
		
	}

}