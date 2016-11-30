/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hadoop.twitter.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author feryandi
 */
public class Preprocess {
    
    public static class UserMapper extends Mapper<LongWritable, Text, Text, UserWritable>{
        private UserWritable result = new UserWritable();
        private Text user_id = new Text();
        private Text follower_id = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context
                      ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            
            user_id.set(itr.nextToken());
            follower_id.set(itr.nextToken());
                        
            result.set(new Double(1.0), user_id);
            context.write(follower_id, result);
        }
    }
    
    public static class UserReducer extends Reducer<Text, UserWritable, Text, UserWritable> {
        private UserWritable result = new UserWritable();
        
        @Override
        public void reduce(Text key, Iterable<UserWritable> values, 
                Context context) throws IOException, InterruptedException {
            Text followee = new Text();
            
            for (UserWritable val : values) {
                String appender = val.getFollowee() + ",";
                followee.append(appender.getBytes(), 0, appender.length());                    
            }
            
            result.set(new Double(1.0), followee);
            context.write(key, result);
        }
    }
    
}
