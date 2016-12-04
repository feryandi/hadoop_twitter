/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hadoop.twitter.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author feryandi
 */
public class UserWritable implements Writable, 
                                     Comparable<UserWritable>, 
                                     Comparator<UserWritable> {
    private DoubleWritable pageRank;
    private Text followee; 
    
    public UserWritable() {
        pageRank = new DoubleWritable(1);
        followee = new Text();
    }
    
    public UserWritable(Double pageRank, String followee) {
        this.pageRank = new DoubleWritable(pageRank);
        this.followee = new Text(followee);
    }
    
    public Double getPageRank() {
        return pageRank.get();
    }

    public void setPageRank(Double pageRank) {
        this.pageRank = new DoubleWritable(pageRank);
    }
    
    public Text getFollowee() {
        return followee;
    }
    
    public void set(Double pageRank, Text follower) {        
        this.pageRank = new DoubleWritable(pageRank);
        this.followee = follower;
    }
    
    @Override
    public void write(DataOutput d) throws IOException {
        pageRank.write(d);
        followee.write(d);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        pageRank.readFields(di);
        followee.readFields(di);
    }
    
    @Override
    public String toString() {
        return pageRank + "\t" + followee.toString();
    }

    @Override
    public int compareTo(UserWritable o) {
        return ((this.getPageRank()).compareTo((o.getPageRank())));
    }

    @Override
    public int compare(UserWritable o1, UserWritable o2) {        
        if (o1.getPageRank() > o2.getPageRank()) {
            return -1;
        } else if (o1.getPageRank() < o2.getPageRank()) {
            return 1;
        } else {
            return 0;
        }
    }
}
