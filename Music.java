import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class Music{
	public static class MapperClass extends Mapper<Object,Text,Text,IntWritable>
	{
		public void map(Object key,Text value,Context context) throws InterruptedException,IOException
		{
			StringTokenizer itr=new StringTokenizer(value.toString(),",");
			while(itr.hasMoreTokens())
			{
				String val=itr.nextToken();
				if(val.charAt(0)=='U')
				{
					
				}
				else
				{
					context.write(new Text(val), new IntWritable(1));					
				}
				itr.nextToken();
				itr.nextToken();
				itr.nextToken();
				itr.nextToken();
				
			}
		}
	}
	
	public static class ReducerClass extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		public static int total=0;
		
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws InterruptedException,IOException
		{
			total=total+1;
//			context.write(key, new IntWritable(total));
		}
		
		public void cleanup(Context context) throws InterruptedException,IOException
		{
			context.write(new Text("total users"), new IntWritable(total));
		}
	}
	
	
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf, "music");
		job.setJarByClass(Music.class);
		job.setMapperClass(MapperClass.class);
//		job.setCombinerClass(ReducerClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}