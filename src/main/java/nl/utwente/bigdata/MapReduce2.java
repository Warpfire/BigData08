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
 * Map Reduce to be done after MapReduce1 and calling -getmerge on the outputs.
 * This MapReduce will merge the different files into 1 by using a single reducer task.
 * @author Adrechsel
 *
 */
public class MapReduce2 {

  public static class LanguageMapper 
       extends Mapper<Object, Text, Text, Text>{
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    		String[] splitValue =value.toString().split("\\s");
      context.write(new Text(splitValue[0]+"\t"+splitValue[1]), new Text(splitValue[2]));
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
  

  

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: exampleTwitter <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "Twitter Reader");
    job.setJarByClass(MapReduce2.class);
    job.setMapperClass(LanguageMapper.class);
    job.setReducerClass(LanguageReducer.class);
    job.setCombinerClass(LanguageReducer.class);
    job.setNumReduceTasks(1);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
