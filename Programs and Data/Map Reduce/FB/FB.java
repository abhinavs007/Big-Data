import java.util.*;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class FB
{
  public static class FBMapper extends MapReduceBase implements
  Mapper <Object, /*Input Key Type */
  Text, /*Input value Type*/
  IntWritable, /*Output key Type*/
  IntWritable> /*Output value Type*/
  {
    //Map function
    boolean flag = false;
    public void map(Object key, Text value, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException
    { 
      String line[] = value.toString().split(",",10);
      if(flag) {
      String date = line[2];
      int shares = Integer.parseInt(line[5]);
      if (date.contains("2017"))
      {
         String y[] = date.split("/",3);
         int month = Integer.parseInt(y[0]);
         output.collect(new IntWritable(month), new IntWritable(shares));
      }
     }
      flag=true;
   }
  }
  //Reducer class
  public static class FBReducer extends MapReduceBase implements
  Reducer<IntWritable,IntWritable, IntWritable, FloatWritable>
  {
    //Reducer Function
    public void reduce(IntWritable key,Iterator<IntWritable> values, OutputCollector<IntWritable, FloatWritable> output, Reporter reporter) throws IOException
    {
    	    int sum=0, total=0;
    	    while(values.hasNext())
    	    {
    	         sum = sum + values.next().get();
    	         total++;
    	    }
    	    output.collect(key, new FloatWritable(sum/(float)total));
    }
  }
  //Main function
  public static void main(String args[]) throws Exception
  {
    // create the object of  Job configuration class
    JobConf conf = new JobConf(FB.class);

    conf.setJobName("Facebook Shares");

    conf.setOutputKeyClass(IntWritable.class);
    
    conf.setOutputValueClass(FloatWritable.class);
    
    conf.setMapOutputKeyClass(IntWritable.class);
    
    conf.setMapOutputValueClass(IntWritable.class);
  
    conf.setMapperClass(FBMapper.class);

    conf.setReducerClass(FBReducer.class);
    
    conf.setInputFormat(TextInputFormat.class);
    
    conf.setOutputFormat(TextOutputFormat.class);

    // Set the input files paths
     FileInputFormat.setInputPaths(conf, new Path(args[0]));
    
    // Set the output file path from 1st argument
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }
}







