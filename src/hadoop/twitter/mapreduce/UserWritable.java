/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hadoop.twitter.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author feryandi
 */
public class UserWritable implements WritableComparable<UserWritable> {
    private LongWritable id = new LongWritable(0);
    private DoubleWritable pageRank = new DoubleWritable(1);
    private Text following = new Text(); 
    
    public UserWritable() {
        id = new LongWritable(0);
        pageRank = new DoubleWritable(1);
        following = new Text();
    }
    
    public UserWritable(Long id) {
        this.id = new LongWritable(id);
        pageRank = new DoubleWritable(1);
        following = new Text();
    }
    
    public UserWritable (Long id, Double pageRank, Text following) {
        this.id = new LongWritable(id);
        this.pageRank = new DoubleWritable(pageRank);
        this.following = following;
    }
    
    public LongWritable getId() {
        return id;
    }

    public void setId(LongWritable id) {
        this.id = id;
    }

    public DoubleWritable getPageRank() {
        return pageRank;
    }

    public void setPageRank(DoubleWritable pageRank) {
        this.pageRank = pageRank;
    }

    public Text getFollowing() {
        return following;
    }
    
    @Override
    public void write(DataOutput d) throws IOException {
        id.write(d);
        pageRank.write(d);
        following.write(d);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        id.readFields(di);
        pageRank.readFields(di);
        following.readFields(di);
    }

    @Override
    public int compareTo(UserWritable o) {
        if ( id.get() < o.getId().get() ) {
            return -1;
        } else if ( id.get() > o.getId().get() ) {
            return 1;
        } else {
            return 0;
        }
    }
    
}
