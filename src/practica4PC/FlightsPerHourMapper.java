package practica4PC;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlightsPerHourMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static int FLIGHTS_POSITION = 9;
	private final static int HOUR_POSITION = 4;

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (!key.equals(0)) {
			String line = value.toString(); 
			
			StringTokenizer tokenizer = new StringTokenizer(line, ",");
			
			Text hour = new Text();
			
			int i = 0;
			
			String token = "";
			
			while (tokenizer.hasMoreTokens()) {
				token = tokenizer.nextToken();
				
				if (i == HOUR_POSITION)
					if (!"NA".equals(token) && !"DepTime".equals(token))
						hour.set(token);
					else
						break;
				else 
					if (i == FLIGHTS_POSITION) {
						if (!"NA".equals(token) && !"FlightNum".equals(token))
							context.write(hour, new IntWritable(Integer.parseInt(token)));
						
						break;
					}
				
				i++;
			}			
		}

	}
}
