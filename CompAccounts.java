import java.io.IOException;
import java.util.*;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.*;

import javax.security.auth.callback.TextInputCallback;

public class CompAccounts {

    public static class Map extends Mapper<LongWritable, Text, Text, Text>{

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            String[] ara = line.split(",");
            System.out.println(line);
            try{
                String orgno = ara[0];
                context.write(new Text(orgno), new Text(line));
            }catch(JSONException e){
                e.printStackTrace();
            }
        }
    }

    public static class Reduce extends Reducer<Text,Text,NullWritable,Text>{

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

            try{
               
                JSONObject res = new JSONObject();
                JSONArray ja = new JSONArray();

                int cnt = 0, jj = 0;
                for(Text val : values){
                    String line = val.toString();
                    String[] ara = new String[9];

                    int i, j = -1, prev = -1;
                    for(i=0; i<line.length(); i++){
                        if(line.charAt(i) == ',' && line.charAt(i-1) != '\\'){
                            ara[++j] = line.substring(prev+1, i);
                            prev = i;
                        }
                    }
                    ara[++j] = line.substring(prev+1, i);
                    jj = j;
                    
                    JSONObject obj = new JSONObject();

                    if(cnt == 0){
                        res.put("orgno", key.toString());
                        res.put("company_name", ara[1]);
                        if(ara[2].length() > 0)
                            res.put("phone_number", ara[2]);
                        if(ara[3].length() > 0)
                            res.put("sni_code", ara[3]);
                        if(ara[4].length() > 0)
                            res.put("sni_text", ara[4]);
                        cnt = 1;
                    }

                    if(j > 5) {
                        obj.put("date_from", ara[5].trim());
                        obj.put("date_to", ara[6]);
                        obj.put("profit_margin_percent", ara[7]);
                        obj.put("net_operating_income", ara[8]);
                        ja.put(obj);
                    }
                }

                if(jj > 5)
                    res.put("accounts", ja);
                
                context.write(NullWritable.get(), new Text(res.toString()));
            }catch(JSONException e){
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: CompanyAcountsJson <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "CombineBooks");
        job.addFileToClassPath(new Path("/user/hduser/json-20180813.jar")); 
        job.setJarByClass(CompAccounts.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
