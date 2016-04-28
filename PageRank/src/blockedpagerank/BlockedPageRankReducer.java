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
    
    private Map<String, String> nodes = new HashMap<>();
    private Map<String, List<String>> be = new HashMap<>();
    private Map<String, Double> bc = new HashMap<>();

    
    @Override
    public void reduce(Text key, Iterator<Text> values,
            OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
        nodes.clear();
        String value = null;
        
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
        
        
    }
    
    /*
    void IterateBlockOnce(B) {
        for( v ∈ B ) { NPR[v] = 0; }
        for( v ∈ B ) {
            for( u where <u, v> ∈ BE ) {
                NPR[v] += PR[u] / deg(u);
            }
            for( u, R where <u,v,R> ∈ BC ) {
                NPR[v] += R;
            }
            NPR[v] = d*NPR[v] + (1-d)/N;
        }
        for( v ∈ B ) { PR[v] = NPR[v]; }
    }
    */
    public void iterateBlockOnce() {
        
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
        
        if (!be.containsKey(srcId)) {
            be.put(srcId, new ArrayList<String>());
        }
        
        be.get(srcId).add(dstId);
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
        
        if (bc.containsKey(dstId)) {
            bc.put(dstId, bc.get(dstId) + Double.parseDouble(prUpdate));
        } else {
            bc.put(dstId, Double.parseDouble(prUpdate));
        }
    }
    
    
    /**
     * Processes the Node information with old PR value.
     * @param value node information, format "srcId oldPr dstIds"
     *              dstIds could be empty
     */
    private void processPR(String value) {
        String[] node = value.split(util.Const.DELIMITER, 2);
        nodes.put(node[0], node[1]);
    }
    
}
