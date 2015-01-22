/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.utwente.bigdata;

import java.io.IOException;
import java.util.Calendar;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.simple.parser.JSONParser;

/**
 * MapReduce which takes tweets and counts the combination of profile language, machine detected language and time.
 * This will only map and reduce a certain period of time with the selection on minutes and the granularity on 10 seconds
 * @author Adrechsel
 *
 */
public class MapReduce5 {

  public static class LanguageMapper 
       extends Mapper<Object, Text, Text, Text>{

    private JSONParser parser = new JSONParser();
    private Map tweet;
    private Text languages = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      try {
        tweet = (Map<String, Object>) parser.parse(value.toString());
      }
      catch (ClassCastException e) {  
        return; // do nothing (we might log this)
      }
      catch (org.json.simple.parser.ParseException e) {  
        return; // do nothing 
      }

      String time=parseAndMatchTime((String)tweet.get("created_at"),context.getConfiguration());
      if(!time.equals("false")){
      languages.set((String) ((Map<String,Object>) tweet.get("user")).get("lang")+"\t"+(String) tweet.get("lang")+"\t"+time);
      context.write(languages, new Text("1"));}
      }
      }
  
  public static class LanguageReducer 
       extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
    	int counter =0;
    	for (Text value : values) {
        counter=counter+(Integer.parseInt(value.toString()));
      }
    	context.write(key, new Text(""+counter));
    }
    }
  

  private static String parseAndMatchTime(String date, Configuration conf){
	  String[] splitdate =date.toString().split("\\s");
	  int year=Integer.parseInt(splitdate[5]);
	  int month=translateMonthCalendar(splitdate[1]);
	  int day=Integer.parseInt(splitdate[2]);
	  int hour=Integer.parseInt(splitdate[3].substring(0, 2));
	  int minute=Integer.parseInt(splitdate[3].substring(3, 5));
	  int second=Integer.parseInt(splitdate[3].substring(6,7)+"0");
	  Calendar start = Calendar.getInstance();
	  start.set(Integer.parseInt(conf.get("year")), translateMonthCalendar(conf.get("month")), Integer.parseInt(conf.get("day")), Integer.parseInt(conf.get("hour")), Integer.parseInt(conf.get("minute")));
	  Calendar end = (Calendar) start.clone();
	  end.add(Calendar.MINUTE, Integer.parseInt(conf.get("duration")));
	  Calendar value = Calendar.getInstance();
	  value.set(year, month, day, hour, minute,second);
	  if(value.compareTo(start)>=0&&value.compareTo(end)<=0){
		  return year+":"+translateMonth(splitdate[1])+":"+day+":"+hour+":"+minute+":"+second;
	  	}
	  else
	  return "false";  
	 }
  
  private static int translateMonthCalendar(String month){
	  int monthOut=0;
	  if(month.equals("Jul")){
		  monthOut=Calendar.JULY;
	  }
	  if(month.equals("Jun")){
		  monthOut=Calendar.JUNE;
	  }
	  return monthOut;  
  }

  private static String translateMonth(String month){
	  String monthOut="";
	  if(month.equals("Jul")){
		  monthOut="07";
	  }
	  if(month.equals("Jun")){
		  monthOut="06";
	  }
	  return monthOut; 
  }
  
  public static void main(String[] args) throws Exception {
	 Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 8) {
        System.err.println("Usage: exampleTwitter <in>  <out> <year> <Month> <Day> <hour> <minute> <duration>");
        System.exit(2);
      }
    Job job = new Job(conf, "Twitter Reader");
    job.getConfiguration().set("year", otherArgs[2]);
    job.getConfiguration().set("month", otherArgs[3]);
    job.getConfiguration().set("day", otherArgs[4]);
    job.getConfiguration().set("hour", otherArgs[5]);
    job.getConfiguration().set("minute", otherArgs[6]);
    job.getConfiguration().set("duration", otherArgs[7]);
    job.setJarByClass(MapReduce5.class);
    job.setMapperClass(LanguageMapper.class);
    job.setReducerClass(LanguageReducer.class);
    job.setCombinerClass(LanguageReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job,
    new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

