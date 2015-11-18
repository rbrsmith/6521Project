package comp6521.mapreduce.join;

import org.apache.hadoop.mapreduce.Partitioner;

import comp6521.mapreduce.join.util.TaggedElement;
import comp6521.mapreduce.join.util.TaggedKey;

public class TaggedJoiningPartitioner extends Partitioner<TaggedKey, TaggedElement> {

    @Override
    public int getPartition(TaggedKey taggedKey, TaggedElement pair, int numPartitions) {
        return taggedKey.getJoinKey().hashCode() % numPartitions;
    }

}
