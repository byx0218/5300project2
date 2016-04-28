package blockedpagerank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class BlockedPageRankMapper extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value,
            OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
        // value format will be: "srcId 1.0 dstId1 dstId2 ... dtsIdn"
        String[] node = value.toString().trim().split(util.Const.DELIMITER, 3);
        String srcId = node[0];
        String dstIds = util.Const.EMPTY;
        double pr = Double.parseDouble(node[1]);
        long srcBlockId = util.Const.blockIdOfNode(Long.parseLong(srcId));
        
        Text outKey = new Text(Long.toString(srcBlockId));
        Text outValue = new Text(srcId + util.Const.SPACE + node[1] + util.Const.SPACE + dstIds);
        output.collect(outKey, outValue);
        
        // this node has out going edges
        if (node.length == 3) {
            dstIds = node[2].trim();
            String[] dstIdArr = dstIds.split(util.Const.DELIMITER);
            String prUpdate = Double.toString(pr / dstIdArr.length);
            
            for (String dstId : dstIdArr) {
                String edge = util.Const.SPACE + srcId + util.Const.SPACE + dstId;
                long dstBlockId = util.Const.blockIdOfNode(Long.parseLong(dstId));
                outKey = new Text(Long.toString(dstBlockId));
                
                if (dstBlockId == srcBlockId) {
                    // two Nodes in the same block
                    outValue = new Text(util.Const.PREFIX_BE + edge);
                    
                } else {
                    // destination Node in another block
                    outValue = new Text(util.Const.PREFIX_BC + edge + util.Const.SPACE + prUpdate);
                }
                
                output.collect(outKey, outValue);
            }
        }
    }

}
