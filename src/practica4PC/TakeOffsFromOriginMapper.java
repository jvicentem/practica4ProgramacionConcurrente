package practica4PC;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TakeOffsFromOriginMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static int FLIGHTS_POSITION = 9;
	private final static int ORIGIN_POSITION = 16; 

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (!key.equals(0)) {
			IntWritable flights = new IntWritable();
			
			String line = value.toString(); 
			
			StringTokenizer tokenizer = new StringTokenizer(line, ",");
			
			int i = 0;
			
			String token = "";
			
			while (tokenizer.hasMoreTokens()) {
				token = tokenizer.nextToken();
				
				if (i == FLIGHTS_POSITION)
					if (!"NA".equals(token) && !"FlightNum".equals(token))
						flights.set(Integer.parseInt(token));	
					else
						break;
				else 
					if (i == ORIGIN_POSITION) {
						if (!"NA".equals(token) && !"Origin".equals(token))
							context.write(new Text(token), flights);
						
						break;
					}
				
				i++;
			}			
		}

	}
}

