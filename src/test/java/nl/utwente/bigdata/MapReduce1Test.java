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
 
public class MapReduce1Test {
 
  private MapDriver<Object, Text, Text, Text> mapDriver;
  private ReduceDriver<Text, Text, Text, Text> reduceDriver;
  private MapReduceDriver<Object, Text, Text, Text, Text, Text> mapReduceDriver; 
 
  @Before
  public void setUp() {
	  MapReduce1.LanguageMapper mapper   = new MapReduce1.LanguageMapper();
	  MapReduce1.LanguageReducer reducer = new MapReduce1.LanguageReducer();
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }
 

  @Test
  public void testMapper() {
    Object key = new Object();
    Text value = new Text("{\"filter_level\":\"medium\",\"contributors\":null,\"text\":\"Whee I'm done\",\"geo\":null,\"retweeted\":false,\"in_reply_to_screen_name\":null,\"possibly_sensitive\":false,\"truncated\":false,\"lang\":\"done\",\"entities\":{\"trends\":[],\"symbols\":[],\"urls\":[{\"expanded_url\":\"http://fifa.to/1rnbRHM\",\"indices\":[52,74],\"display_url\":\"fifa.to/1rnbRHM\",\"url\":\"http://t.co/Uy6cVbSAol\"}],\"hashtags\":[],\"user_mentions\":[]},\"in_reply_to_status_id_str\":null,\"id\":484645534812995588,\"source\":\"<a href=\\\"http://sproutsocial.com\\\" rel=\\\"nofollow\\\">Sprout Social<\\/a>\",\"in_reply_to_user_id_str\":null,\"favorited\":false,\"in_reply_to_status_id\":null,\"retweet_count\":0,\"created_at\":\"Thu Jul 03 10:31:14 +0000 2014\",\"in_reply_to_user_id\":null,\"favorite_count\":0,\"id_str\":\"484645534812995588\",\"place\":null,\"user\":{\"location\":\"\",\"default_profile\":false,\"statuses_count\":292,\"profile_background_tile\":true,\"lang\":\"done\",\"profile_link_color\":\"009999\",\"profile_banner_url\":\"https://pbs.twimg.com/profile_banners/2460435709/1402925521\",\"id\":2460435709,\"following\":null,\"favourites_count\":1,\"protected\":false,\"profile_text_color\":\"333333\",\"verified\":false,\"description\":null,\"contributors_enabled\":false,\"profile_sidebar_border_color\":\"EEEEEE\",\"name\":\"Grosvenor G Southend\",\"profile_background_color\":\"131516\",\"created_at\":\"Wed Apr 23 22:44:51 +0000 2014\",\"default_profile_image\":false,\"followers_count\":2661,\"profile_image_url_https\":\"https://pbs.twimg.com/profile_images/459100884174004225/RSq5r3pg_normal.jpeg\",\"geo_enabled\":false,\"profile_background_image_url\":\"http://abs.twimg.com/images/themes/theme14/bg.gif\",\"profile_background_image_url_https\":\"https://abs.twimg.com/images/themes/theme14/bg.gif\",\"follow_request_sent\":null,\"url\":null,\"utc_offset\":null,\"time_zone\":null,\"notifications\":null,\"profile_use_background_image\":true,\"friends_count\":761,\"profile_sidebar_fill_color\":\"EFEFEF\",\"screen_name\":\"GCSouthend\",\"id_str\":\"2460435709\",\"profile_image_url\":\"http://pbs.twimg.com/profile_images/459100884174004225/RSq5r3pg_normal.jpeg\",\"listed_count\":1,\"is_translator\":false},\"coordinates\":null}");
    mapDriver.withInput(key, value);
    mapDriver.withOutput(new Text("done\tdone"), new Text("1"));
    mapDriver.runTest();
  }
 

  @Test
  public void testReducer() {
    List<Text> values = new ArrayList<Text>();
    values.add(new Text("1"));
    values.add(new Text("2"));
    values.add(new Text("3"));
    reduceDriver.withInput(new Text("nl en"), values);
    reduceDriver.withOutput(new Text("nl en"), new Text("6"));
    reduceDriver.runTest();
  }


  @Test
  public void testMapReduce() {
    Object key = new Object();
    Text value = new Text("{\"filter_level\":\"medium\",\"contributors\":null,\"text\":\"Whee I'm done\",\"geo\":null,\"retweeted\":false,\"in_reply_to_screen_name\":null,\"possibly_sensitive\":false,\"truncated\":false,\"lang\":\"done\",\"entities\":{\"trends\":[],\"symbols\":[],\"urls\":[{\"expanded_url\":\"http://fifa.to/1rnbRHM\",\"indices\":[52,74],\"display_url\":\"fifa.to/1rnbRHM\",\"url\":\"http://t.co/Uy6cVbSAol\"}],\"hashtags\":[],\"user_mentions\":[]},\"in_reply_to_status_id_str\":null,\"id\":484645534812995588,\"source\":\"<a href=\\\"http://sproutsocial.com\\\" rel=\\\"nofollow\\\">Sprout Social<\\/a>\",\"in_reply_to_user_id_str\":null,\"favorited\":false,\"in_reply_to_status_id\":null,\"retweet_count\":0,\"created_at\":\"Thu Jul 03 10:31:14 +0000 2014\",\"in_reply_to_user_id\":null,\"favorite_count\":0,\"id_str\":\"484645534812995588\",\"place\":null,\"user\":{\"location\":\"\",\"default_profile\":false,\"statuses_count\":292,\"profile_background_tile\":true,\"lang\":\"done\",\"profile_link_color\":\"009999\",\"profile_banner_url\":\"https://pbs.twimg.com/profile_banners/2460435709/1402925521\",\"id\":2460435709,\"following\":null,\"favourites_count\":1,\"protected\":false,\"profile_text_color\":\"333333\",\"verified\":false,\"description\":null,\"contributors_enabled\":false,\"profile_sidebar_border_color\":\"EEEEEE\",\"name\":\"Grosvenor G Southend\",\"profile_background_color\":\"131516\",\"created_at\":\"Wed Apr 23 22:44:51 +0000 2014\",\"default_profile_image\":false,\"followers_count\":2661,\"profile_image_url_https\":\"https://pbs.twimg.com/profile_images/459100884174004225/RSq5r3pg_normal.jpeg\",\"geo_enabled\":false,\"profile_background_image_url\":\"http://abs.twimg.com/images/themes/theme14/bg.gif\",\"profile_background_image_url_https\":\"https://abs.twimg.com/images/themes/theme14/bg.gif\",\"follow_request_sent\":null,\"url\":null,\"utc_offset\":null,\"time_zone\":null,\"notifications\":null,\"profile_use_background_image\":true,\"friends_count\":761,\"profile_sidebar_fill_color\":\"EFEFEF\",\"screen_name\":\"GCSouthend\",\"id_str\":\"2460435709\",\"profile_image_url\":\"http://pbs.twimg.com/profile_images/459100884174004225/RSq5r3pg_normal.jpeg\",\"listed_count\":1,\"is_translator\":false},\"coordinates\":null}");
    mapReduceDriver.withInput(key, value);
    mapReduceDriver.withOutput(new Text("done\tdone"), new Text("1"));
    mapReduceDriver.runTest();
  }

}
