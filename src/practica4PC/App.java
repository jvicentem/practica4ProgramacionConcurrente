package practica4PC;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

//./hadoop jar ./jars/practica4.jar practica4PC.App ./jars/1987.csv ./jars/1988.csv ./jars/1 ./jars/2 ./jars/3 ./jars/4 ./jars/5


public class App {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			Configuration conf = new Configuration();
			
			//1. Para cada año,	el retraso acumulado en	cuanto al despegue y al aterrizaje.
			Job job1 = Job.getInstance(conf, "DeparturesAndArrivalsTimePerYear");
			job1.setJarByClass(App.class);
			
			job1.setMapperClass(DeparturesDelayMapper.class);
			job1.setMapperClass(ArrivalsDelayMapper.class);
			
			job1.setMapOutputKeyClass(IntWritable.class);
			job1.setMapOutputValueClass(IntWritable.class);
			
			job1.setCombinerClass(DeparturesAndArrivalsDelayReducer.class);
			job1.setReducerClass(DeparturesAndArrivalsDelayReducer.class);
			
			MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class);
			MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class);
			FileOutputFormat.setOutputPath(job1, new Path(args[2]));		
			
			
			//2. Cuántos vuelos	se han producido en	cada mes (considerando ambos años).
			Job job2 = Job.getInstance(conf, "FlightsPerMonth");
			job2.setJarByClass(App.class);
			
			job2.setMapperClass(FlightsPerMonthMapper.class);
			
			job2.setMapOutputKeyClass(IntWritable.class);
			job2.setMapOutputValueClass(IntWritable.class);
			
			job2.setCombinerClass(FlightsPerMonthReducer.class);
			job2.setReducerClass(FlightsPerMonthReducer.class);
			
			job2.setOutputKeyClass(IntWritable.class);
			job2.setOutputValueClass(IntWritable.class);
			
			MultipleInputs.addInputPath(job2, new Path(args[0]), TextInputFormat.class);
			MultipleInputs.addInputPath(job2, new Path(args[1]), TextInputFormat.class);
			FileOutputFormat.setOutputPath(job2, new Path(args[3]));
			
			//3. Cuántos vuelos	han	salido cada	hora (considerando ambos años).
			Job job3 = Job.getInstance(conf, "FlightsPerHour");
			job3.setJarByClass(App.class);
			
			job3.setMapperClass(FlightsPerHourMapper.class);
			
			job3.setMapOutputKeyClass(Text.class);
			job3.setMapOutputValueClass(IntWritable.class);
			
			job3.setCombinerClass(FlightsPerHourReducer.class);
			job3.setReducerClass(FlightsPerHourReducer.class);
			
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(IntWritable.class);
			
			MultipleInputs.addInputPath(job3, new Path(args[0]), TextInputFormat.class);
			MultipleInputs.addInputPath(job3, new Path(args[1]), TextInputFormat.class);
			FileOutputFormat.setOutputPath(job3, new Path(args[4]));
			
			//4. La	distancia recorrida acumulada para cada día de la semana.
			Job job4 = Job.getInstance(conf, "DistancePerWeekday");
			job4.setJarByClass(App.class);
			
			job4.setMapperClass(DistancePerWeekdayMapper.class);
			
			job4.setMapOutputKeyClass(IntWritable.class);
			job4.setMapOutputValueClass(IntWritable.class);
			
			job4.setCombinerClass(DistancePerWeekdayReducer.class);
			job4.setReducerClass(DistancePerWeekdayReducer.class);
			
			job4.setOutputKeyClass(IntWritable.class);
			job4.setOutputValueClass(IntWritable.class);
			
			MultipleInputs.addInputPath(job4, new Path(args[0]), TextInputFormat.class);
			MultipleInputs.addInputPath(job4, new Path(args[1]), TextInputFormat.class);
			FileOutputFormat.setOutputPath(job4, new Path(args[5]));
			
			//5. Cuántos vuelos	han	salido de cada origen diferente
			Job job5 = Job.getInstance(conf, "TakeOffsPerOrigin");
			job5.setJarByClass(App.class);
			
			job5.setMapperClass(TakeOffsFromOriginMapper.class);
			
			job5.setMapOutputKeyClass(Text.class);
			job5.setMapOutputValueClass(IntWritable.class);
			
			job5.setCombinerClass(TakeOffsFromOriginReducer.class);
			job5.setReducerClass(TakeOffsFromOriginReducer.class);
			
			job5.setOutputKeyClass(Text.class);
			job5.setOutputValueClass(IntWritable.class);
			
			MultipleInputs.addInputPath(job5, new Path(args[0]), TextInputFormat.class);
			MultipleInputs.addInputPath(job5, new Path(args[1]), TextInputFormat.class);
			FileOutputFormat.setOutputPath(job5, new Path(args[6]));
			
			System.exit(job1.waitForCompletion(true)
						&& 
						job2.waitForCompletion(true)
						&&
						job3.waitForCompletion(true)
						&&
						job4.waitForCompletion(true)
						&&
						job5.waitForCompletion(true) ? 0 : 1);	
	}
}

