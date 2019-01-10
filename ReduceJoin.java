import java.util.*;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

 
 public class ReduceJoin {
	 public static class AccountsMapper extends Mapper <Object, Text, Text, Text>
	 {
		 public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		 {
		 	String line = value.toString();

		 	if(!line.contains("ORG_NUMBER")) {  // skipping header line, key.get() method is not wotking for weird reason! 
			 	String[] fields = line.split(",");
			 	String orgno = fields[1];
				String date_from = fields[2];
				String date_to = fields[3];
				String profit_margin_percent = fields[4];
				String net_operating_income = fields[5];

				String str = date_from+","+date_to+","+profit_margin_percent+","+net_operating_income;
				System.err.println(str);
				System.out.println(str);

				/*
	            JSONObject obj = new JSONObject();
	            obj.put("date_from", date_from);
	            obj.put("date_to", date_to );
	            obj.put("profit_margin_percent", profit_margin_percent);
	            obj.put("net_operating_income", net_operating_income);
	            */
	            
	            context.write(new Text(orgno), new Text("B"+str));
       		}

		 }
	 }


	 public static class CompaniesMapper extends Mapper <Object, Text, Text, Text>
	 {
		 public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		 {
		 	String line = value.toString();

		 	if(!line.contains("ORG_NUMBER")) { // skipping header line, key.get() method is not wotking for weird reason! 

		        String[] fields = new String[5];

		        int i, j = -1, prev = -1;
		        for(i=0; i<line.length(); i++){
		        	if(line.charAt(i) == ',' && line.charAt(i-1) != '\\'){
		        	    fields[++j] = line.substring(prev+1, i);
		        		prev = i;
		        	}
		        }
		        fields[++j] = line.substring(prev+1, i);

				String orgno = fields[0];
				String company_name = fields[1];
				String phone_number = fields[2];
				String sni_code = fields[3];
				String sni_text = fields[4];
				String str = orgno+","+company_name+","+phone_number+","+sni_code+","+sni_text+",";
				
				/*
	            JSONObject obj = new JSONObject();
	            obj.put("company_name", company_name);
	            obj.put("phone_number", phone_number);
	            obj.put("sni_code", sni_code);
	            obj.put("sni_text", sni_text);
	            */
	            
	            context.write(new Text(orgno), new Text("A"+str));
	        }
		 }
	 }
 
	 public static class ReduceJoinReducer extends Reducer <Text, Text, Text, Text>
	 {
	 	private ArrayList<Text> listA = new ArrayList<Text>();
		private ArrayList<Text> listB = new ArrayList<Text>();

		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		 {


		 	// Clear our lists
			listA.clear();
			listB.clear();

			// iterate through all our values, binning each record based on what
			// it was tagged with make sure to remove the tag!
			for (Text t : values) {
				if (t.charAt(0) == 'A') {
					listA.add(new Text(t.toString().substring(1)));
				} else if (t.charAt(0) == 'B') {
					listB.add(new Text(t.toString().substring(1)));
				}
			}

			// LEFT INNER JOIN, companies with accounts
			// For each entry in A,
			for (Text A : listA) {
				// If list B is not empty, join A and B
				if (!listB.isEmpty()) {
					for (Text B : listB) {
						context.write(A, B);
					}
				} else {
					// Else, output A by itself
					context.write(A, new Text(""));
				}
			}
		 }
	 }
 
	 public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		/*if (args.length != 3) {
			System.err.println("Usage: ReduceSideJoin <account data> <company data> <out>");
			System.exit(1);
		}*/

		Job job = new Job(conf, "Reduce side join");
		job.setJarByClass(ReduceJoin.class);
		job.setReducerClass(ReduceJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, AccountsMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, CompaniesMapper.class);
		
		Path outputPath = new Path(args[2]);

		FileOutputFormat.setOutputPath(job, outputPath);
		//outputPath.getFileSystem(conf).delete(outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	 }
 }
