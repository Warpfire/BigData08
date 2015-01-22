package nl.utwente.bigdata;
import java.util.ArrayList;
import java.util.List;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;


import org.junit.Before;
import org.junit.Test;

import nl.utwente.bigdata.MapReduce1;
 
public class MapReduce2Test {
 
  private MapDriver<Object, Text, Text, Text> mapDriver;
  private ReduceDriver<Text, Text, Text, Text> reduceDriver;
  private MapReduceDriver<Object, Text, Text, Text, Text, Text> mapReduceDriver; 
 
  @Before
  public void setUp() {
	  MapReduce2.LanguageMapper mapper   = new MapReduce2.LanguageMapper();
	  MapReduce2.LanguageReducer reducer = new MapReduce2.LanguageReducer();
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }
 

  @Test
  public void testMapper() {
    Object key = new Object();
    Text value = new Text("nl\tnl\t55");
    mapDriver.withInput(key, value);
    mapDriver.withOutput(new Text("nl\tnl"), new Text("55"));
    mapDriver.runTest();
  }
 

  @Test
  public void testReducer() {
    List<Text> values = new ArrayList<Text>();
    values.add(new Text("1"));
    values.add(new Text("2"));
    values.add(new Text("3"));
    reduceDriver.withInput(new Text("nl\ten"), values);
    reduceDriver.withOutput(new Text("nl\ten"), new Text("6"));
    reduceDriver.runTest();
  }


  @Test
  public void testMapReduce() {
    Object key = new Object();
    Text value = new Text("nl\tnl\t55");
    mapReduceDriver.withInput(key, value);
    mapReduceDriver.withOutput(new Text("nl\tnl"), new Text("55"));
    mapReduceDriver.runTest();
  }

}
