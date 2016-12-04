/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hadoop.twitter.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Collections;
import java.util.Comparator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author feryandi
 */
public class Ranking {
    
    public static class RankingMapper extends Mapper<LongWritable, Text, Text, UserWritable>{
        private UserWritable result = new UserWritable();
        private Text user_id = new Text();
        private Text page_rank = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context
                      ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            
            user_id.set(itr.nextToken());
            page_rank.set(itr.nextToken());
            
            result.set(Double.parseDouble(page_rank.toString()), user_id);
            context.write(new Text("0"), result);
        }
    }
    
    public static class RankingReducer extends Reducer<Text, UserWritable, Text, UserWritable> {
        private UserWritable result = new UserWritable();
        
        @Override
        public void reduce(Text key, Iterable<UserWritable> values, 
                Context context) throws IOException, InterruptedException {
            ArrayList<UserWritable> ranked = new ArrayList<>();
            
            for (UserWritable val : values) {                
                UserWritable user = new UserWritable(val.getPageRank(), val.getFollowee().toString());
                ranked.add(user);
                Collections.sort(ranked, new Comparator<UserWritable>() {
                    @Override
                    public int compare(UserWritable u1, UserWritable u2) {
                      if (u1.getPageRank() > u2.getPageRank()) {
                        return -1;
                      } else if (u1.getPageRank() < u2.getPageRank()) {
                        return 1;
                      } else {
                        return 0;
                      }
                    }
                });
                
                if (ranked.size() > 5) {
                    ranked.remove(ranked.size() - 1);
                }
            }
            
            for (UserWritable r: ranked) {
                UserWritable user = new UserWritable(r.getPageRank(), r.getFollowee().toString());
                context.write(key, user);    
            }
        }
    }
    
}
