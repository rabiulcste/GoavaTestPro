import java.io.IOException;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONException;
import org.json.JSONObject;
 
public class MultipleOutputsJson extends Configured implements Tool
{	
    public static class MultipleOutputsMapper extends Mapper<LongWritable, Text, Text, Text> 
	{			
		public void map(LongWritable mkey, Text mvalue, Context context) throws IOException, InterruptedException
        {
			try
			{
				JSONObject jsonObj = new JSONObject(mvalue.toString());

				//parse the input data with JSONObject	
				String orgno = (String)jsonObj.get("orgno");

				//emitting directory structure as key and input record as value.
				context.write(new Text(orgno), new Text(mvalue.toString()));	          			    
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
				
	public static class MultipleOutputsReducer extends Reducer<Text, Text ,NullWritable, Text> 
	{			
		private MultipleOutputs<NullWritable,Text> multipleOutputs;
		
		public void setup(Context context) throws IOException, InterruptedException
		{
			multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
		}

		public void reduce(Text rkey, Iterable<Text> rvalue, Context context) throws IOException, InterruptedException
		{							
			for(Text value : rvalue) {				              		             		               
				multipleOutputs.write(NullWritable.get(), value, rkey.toString());		          		           
			}		           
		}

		public void cleanup(Context context) throws IOException, InterruptedException
		{
			multipleOutputs.close();
		}		
	}	
		
	public int run(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();		
		String inputpath = args[0];
		String outputpath = args[1];
		FileSystem fs = FileSystem.get(conf);
 
		Job job = new Job(conf, "MultipleOutputs");		
		job.addFileToClassPath(new Path("/user/hduser/json-20180813.jar")); 
		job.setJarByClass(MultipleOutputsJson.class);
		
		job.setMapperClass(MultipleOutputsMapper.class);
		job.setReducerClass(MultipleOutputsReducer.class);
 
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
 
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);  

		job.setNumReduceTasks(2);
		FileInputFormat.addInputPath(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));
				
		if(fs.exists(new Path(outputpath)))
		{
			fs.delete(new Path(outputpath), true);
		}
		
		job.waitForCompletion(true);			
		return 0;	   
	}
	
	public static void main(String[] args) throws Exception 
	{
		int exitCode = ToolRunner.run(new MultipleOutputsJson(), args);
		System.exit(exitCode);
    }
}
