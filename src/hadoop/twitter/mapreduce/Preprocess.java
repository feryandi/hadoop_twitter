/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hadoop.twitter.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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
            Text following = new Text();
            
            for (UserWritable val : values) {
                String appender = val.getFollowing() + ",";
                following.append(appender.getBytes(), 0, appender.length());
            }
            
            result.set(new Double(1.0), following);
            context.write(key, result);
        }
    }
    
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "(feryandi) Preprocess");
            job.setJarByClass(Preprocess.class);
            job.setMapperClass(UserMapper.class);
            job.setCombinerClass(UserReducer.class);
            job.setReducerClass(UserReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(UserWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception ex) {
            Logger.getLogger(Preprocess.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
