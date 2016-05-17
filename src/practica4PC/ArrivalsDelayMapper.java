package practica4PC;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ArrivalsDelayMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
	private final static int ARRIVAL_DELAY_POSITION = 14;
	private final static int YEAR_POSITION = 0;
	


	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (!key.equals(0)) {
			String line = value.toString(); 
			StringTokenizer tokenizer = new StringTokenizer(line, ",");
			
			IntWritable year = new IntWritable();
			
			int i = 0;
			String token = "";
			
			while (tokenizer.hasMoreTokens()) {
				token = tokenizer.nextToken();
				
				if (i == YEAR_POSITION)
					if (!"NA".equals(token) && !"Year".equals(token))
						year.set(Integer.parseInt(token));				
					else 
						break;
				else 
					if (i == ARRIVAL_DELAY_POSITION) {
						if (!"NA".equals(token) && !"ArrDelay".equals(token))
							context.write(year, new IntWritable(Integer.parseInt(token)));				
						
						break;
					}					

				
				i++;
			}			
		}

	}
}


