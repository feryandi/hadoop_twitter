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
public class FollowerCount {
    
    public static class UserMapper extends Mapper<Object, Text, UserWritable, UserWritable>{
        private Long user_id;
        private Long follower_id;

        @Override
        public void map(Object key, Text value, Context context
                      ) throws IOException, InterruptedException {
            String[] vals = value.toString().split("\\s+");
            user_id = Long.parseLong(vals[0]);
            follower_id = Long.parseLong(vals[1]);
            
            UserWritable k = new UserWritable(follower_id);
            UserWritable v = new UserWritable(user_id);
            
            context.write(k, v);
        }
    }
    
    public static class UserReducer extends Reducer<UserWritable, UserWritable, UserWritable, UserWritable> {
        @Override
        public void reduce(UserWritable key, Iterable<UserWritable> values, 
                Context context) throws IOException, InterruptedException {
            Text following = new Text();
            
            for (UserWritable val : values) {
                String uid = (val.getId()).toString();
                following.append(uid.getBytes(), 0, uid.length());
            }
            
            context.write(new UserWritable(key.getId().get()), new UserWritable(key.getId().get(), new Double(1.0), following));
        }
    }
    
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "(feryandi) Test MapReduce");
            job.setJarByClass(FollowerCount.class);
            job.setMapperClass(UserMapper.class);
            job.setCombinerClass(UserReducer.class);
            job.setReducerClass(UserReducer.class);
            job.setOutputKeyClass(UserWritable.class);
            job.setOutputValueClass(UserWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception ex) {
            Logger.getLogger(FollowerCount.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
