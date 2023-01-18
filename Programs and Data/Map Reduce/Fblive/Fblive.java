import java.util.*;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Fblive
{
  public static class FbliveMapper extends MapReduceBase implements
  Mapper <Object, /*Input Key Type */
  Text, /*Input value Type*/
  Text, /*Output key Type*/
  IntWritable> /*Output value Type*/
  {
    //Map function
    boolean flag = false;
    public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
    { 
      String line = value.toString();
      if(flag) {
      StringTokenizer s = new StringTokenizer(line,",");
      String id = s.nextToken(); 
      String type = s.nextToken();
      String date = s.nextToken();
      int count = 0, likes = 0;  
      while(count < 4) 
       {
          likes = Integer.parseInt(s.nextToken());
          count++;
       }
      if (type.equals("video") && date.startsWith("2") && date.contains("2018"))
          output.collect(new Text("Likes"), new IntWritable(likes));
      }
      flag=true;
    }
  }
  //Reducer class
  public static class FbliveReducer extends MapReduceBase implements
  Reducer<Text,IntWritable, Text, IntWritable>
  {
    //Reducer Function
    public void reduce(Text key,Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
    {
      int add = 0;    
      while(values.hasNext())
        add = add + values.next().get();
      
      output.collect(key, new IntWritable(add));
    }
  }
  //Main function
  public static void main(String args[]) throws Exception
  {
    // create the object of  Job configuration class
    JobConf conf = new JobConf(Fblive.class);

    conf.setJobName("Facebook Likes");

    conf.setOutputKeyClass(Text.class);
    
    conf.setOutputValueClass(IntWritable.class);
    
    conf.setMapperClass(FbliveMapper.class);

    conf.setReducerClass(FbliveReducer.class);
    
    conf.setInputFormat(TextInputFormat.class);
    
    conf.setOutputFormat(TextOutputFormat.class);

    // Set the input files paths
     FileInputFormat.setInputPaths(conf, new Path(args[0]));
    
    // Set the output file path from 1st argument
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }
}







