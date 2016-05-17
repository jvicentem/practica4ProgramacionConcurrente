package practica4PC;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlightsPerMonthMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
	private final static int FLIGHTS_POSITION = 9;
	private final static int MONTH_POSITION = 1;

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (!key.equals(0)) {
			String line = value.toString(); 
			
			StringTokenizer tokenizer = new StringTokenizer(line, ",");
			
			IntWritable month = new IntWritable();
			
			int i = 0;
			String token = "";
			
			while (tokenizer.hasMoreTokens()) {
				token = tokenizer.nextToken();
				
				if (i == MONTH_POSITION)
					if (!"NA".equals(token) && !"Month".equals(token))
						month.set(Integer.parseInt(token));		
					else
						break;
				else 
					if (i == FLIGHTS_POSITION) {
						if (!"NA".equals(token) && !"FlightNum".equals(token))
							context.write(month, new IntWritable(Integer.parseInt(token)));
						
						break;
					}
				
				i++;
			}			
		}
	}
}