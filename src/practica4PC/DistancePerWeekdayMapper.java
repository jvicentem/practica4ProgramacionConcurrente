package practica4PC;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistancePerWeekdayMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
	private final static int DISTANCE_POSITION = 18;
	private final static int WEEKDAY_POSITION = 3;

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (!key.equals(0)) {
			String line = value.toString(); 
			
			StringTokenizer tokenizer = new StringTokenizer(line, ",");
			
			IntWritable weekDay = new IntWritable();
			
			int i = 0;
			
			String token = "";
			
			while (tokenizer.hasMoreTokens()) {
				token = tokenizer.nextToken();
				
				if (i == WEEKDAY_POSITION)
					if (!"NA".equals(token) && !"DayOfWeek".equals(token))
						weekDay.set(Integer.parseInt(token));		
					else
						break;
				else 
					if (i == DISTANCE_POSITION) {
						if (!"NA".equals(token) && !"Distance".equals(token))
							context.write(weekDay, new IntWritable(Integer.parseInt(token)));
						
						break;
					}
				
				i++;
			}			
		}

	}
}

