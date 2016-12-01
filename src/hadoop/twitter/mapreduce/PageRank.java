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
public class PageRank {
        
    public static class RankMapper extends Mapper<LongWritable, Text, Text, UserWritable>{
        private UserWritable result = new UserWritable();
        private Text user_id = new Text();
        private Text page_rank = new Text("1");
        private Text followees = new Text(",");
        private Text followee = new Text();
        
        @Override
        public void map(LongWritable key, Text value, Context context
                      ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            
            user_id.set(itr.nextToken());
            
            if (itr.hasMoreTokens()) {
                page_rank.set(itr.nextToken());
            }
            
            String[] list_followee = new String[0];
            if (itr.hasMoreTokens()) {
                followees.set(itr.nextToken());
                list_followee = (followees.toString()).split(",");
            }
            
            result.set((Double) 0.0, followees);
            context.write(user_id, result);
            
            result.set(Double.parseDouble(page_rank.toString())/list_followee.length, new Text());
            for(String f: list_followee) {
                followee.set(f);
                if (!followee.equals(0)) {
                    context.write(followee, result);
                }
            }
        }
    }
    
    public static class RankReducer extends Reducer<Text, UserWritable, Text, UserWritable> {
        private UserWritable result = new UserWritable();
        private Text followees = new Text();
        
        @Override
        public void reduce(Text key, Iterable<UserWritable> values, 
                Context context) throws IOException, InterruptedException {
            Double d = 0.85;
            Double rank = 1 - d;
            
            for (UserWritable val : values) {
                if (!val.getFollowee().equals(new Text())) {
                    followees.set(val.getFollowee());
                } else {
                    rank += (val.getPageRank()) * d; 
                }
            }
            
            result.set(rank, followees);
            context.write(key, result);
        }
    }
}
