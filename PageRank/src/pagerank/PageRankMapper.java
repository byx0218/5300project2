package pagerank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PageRankMapper extends MapReduceBase
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
        
        // this node has out going edges
        if (node.length == 3) {
            dstIds = node[2].trim();
            String[] dstIdArr = dstIds.split(util.Const.DELIMITER);
            String prUpdate = Double.toString(pr / dstIdArr.length);
            
            for (String dstId : dstIdArr) {
                Text outKey = new Text(dstId);
                Text outValue = new Text(prUpdate);
                output.collect(outKey, outValue);
            }
        }
        
        Text outKey = new Text(srcId);
        Text outDstIds = new Text(util.Const.PREFIX_DST_IDS + util.Const.SPACE + dstIds);
        Text outOldPR = new Text(util.Const.PREFIX_PR + util.Const.SPACE + node[1]);
        output.collect(outKey, outDstIds);
        output.collect(outKey, outOldPR);
    }

}
