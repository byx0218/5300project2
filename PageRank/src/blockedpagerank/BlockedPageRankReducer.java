package blockedpagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class BlockedPageRankReducer extends MapReduceBase
        implements Reducer<Text, Text, Text, Text> {
    
    private Map<String, Double> oldPRs = new HashMap<>();
    private Map<String, Double> newPRs = new HashMap<>();
    private Map<String, String> nodeDstIds = new HashMap<>();
    private Map<String, Integer> nodeOutDegree = new HashMap<>();
    private Map<String, List<String>> be = new HashMap<>();
    private Map<String, Double> bc = new HashMap<>();

    
    @Override
    public void reduce(Text key, Iterator<Text> values,
            OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
        clear();
        String value = null;
        double iterResidual = Double.MAX_VALUE;
        double blockResidual = 0.0;
        
        while (values.hasNext()) {
            value = values.next().toString();
            
            if (value.isEmpty()) {
                break;
            }
            
            if (value.startsWith(util.Const.PREFIX_BE)) {
                processBE(value);
            } else if (value.startsWith(util.Const.PREFIX_BC)) {
                processBC(value);
            } else {
                processPR(value);
            }
        }
        
        while (iterResidual > util.Const.THRESHOLD) {
//            if (iterations >= util.Const.ITERATIONS) {
//                break;
//            }
            
            iterResidual = iterateBlockOnce();
        }
        
        for (String v : newPRs.keySet()) {
            blockResidual += Math.abs(oldPRs.get(v) - newPRs.get(v)) / newPRs.get(v);
        }
        
        blockResidual /= oldPRs.size();
        reporter.incrCounter(BlockedPageRank.Residual.ERROR,
                (long) Math.floor(blockResidual * util.Const.AMP));
        
        for (String v : newPRs.keySet()) {
            Text outKey = new Text(v);
            Text outValue = new Text(Double.toString(newPRs.get(v)) + util.Const.SPACE + nodeDstIds.get(v));
            output.collect(outKey, outValue);
        }
    }

    
    /**
     * 
     * @return
     */
    private double iterateBlockOnce() {
        double iterResidual = 0.0;
        
        for (String v : oldPRs.keySet()) {
            double nPR = 0.0;
            
            if (be.containsKey(v)) {
                for (String u : be.get(v)) {
                    nPR += newPRs.get(u) / nodeOutDegree.get(u);
                }
            }
            
            if (bc.containsKey(v)) {
                nPR += bc.get(v);
            }
            
            nPR *= util.Const.D;
            nPR += (1 - util.Const.D) / util.Const.N;
            iterResidual += Math.abs(newPRs.get(v) - nPR) / nPR;
            newPRs.put(v, nPR);
        }
        
        return iterResidual / oldPRs.size();
    }
    
    
    /**
     * Processes the BE edge.
     * @param value edge information, format "be srcId dstId"
     *              srcId and dstId both in the same block, B
     */
    private void processBE(String value) {
        String[] edge = value.split(util.Const.DELIMITER);
        String srcId = edge[1];  // u
        String dstId = edge[2];  // v
        
        if (!be.containsKey(dstId)) {
            be.put(dstId, new ArrayList<String>());
        }
        
        be.get(dstId).add(srcId);
    }
    
    
    /**
     * Processes the BC edge. 
     * @param value edge information, format "bc srcId dstId pr(srcId)/deg(srcId)"
     *              srcId not in block B, dstId in B
     */
    private void processBC(String value) {
        String[] edge = value.split(util.Const.DELIMITER);
        String dstId = edge[2];
        String prUpdate = edge[3];
        
        if (!bc.containsKey(dstId)) {
            bc.put(dstId, 0.0);
        }
        
        bc.put(dstId, bc.get(dstId) + Double.parseDouble(prUpdate));
    }
    
    
    /**
     * Processes the Node information with old PR value.
     * @param value node information, format "srcId oldPr dstIds"
     *              dstIds could be empty
     */
    private void processPR(String value) {
        String[] node = value.split(util.Const.DELIMITER, 3);
        double oldPR = Double.parseDouble(node[1]);
        oldPRs.put(node[0], oldPR);
        newPRs.put(node[0], oldPR);
        nodeDstIds.put(node[0], node[2]);
        nodeOutDegree.put(node[0], node[2].split(util.Const.DELIMITER).length);
    }
    
    
    /**
     * Clears up all the hash tables in this Reducer instance.
     */
    private void clear() {
        oldPRs.clear();
        newPRs.clear();
        nodeDstIds.clear();
        be.clear();
        bc.clear();
    }
}
