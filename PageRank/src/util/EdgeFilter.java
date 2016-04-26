package util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;

public class EdgeFilter {
    
    private static final String SPACE = " ";
    
    /** Regular expression separator string. */
    private static final String DELIMITER = "\\s+";
    
    /** Files to read or write. */
    private static final String EDGES = "edges.txt";
    private static final String FLTRED = "filtered_edges.txt";
    
    /** Filter parameters from NetID "xg95". */
    private static final double NET_ID = 0.59;
    private static final double REJECT_MIN = 0.9 * NET_ID;
    private static final double REJECT_MAX = REJECT_MIN + 0.01;
    
    private static final double INIT_PR = 1.0 / 685230;
    
    /**
     * Constructs an Edge filter.
     */
    public static void main(String[] args) {
        Map<Long, Node> nodeTbl1 = processEdges();
        filterEdges();
        Map<Long, Node> nodeTbl2 = processFilteredEdges();
        
        System.out.println(nodeTbl1.size());
        System.out.println(nodeTbl2.size());
        
        for (Long srcId : nodeTbl1.keySet()) {
            if (nodeTbl2.containsKey(srcId)) {
                Node n1 = nodeTbl1.get(srcId);
                Node n2 = nodeTbl2.get(srcId);
                
                if (!n1.getDestinationList().equals(n2.getDestinationList())) {
                    System.out.println("list error " + srcId);
                    break;
                }
            } else {
                System.out.println("key error " + srcId);
                break;
            }
        }
    }
    
    /**
     * Processes the Edges. Filter out the Edges with values within
     * REJECT_MIN and REJECT_MAX. Returns all the Nodes in a map.
     * @return map of all Nodes
     */
    public static Map<Long, Node> processEdges() {
        Map<Long, Node> nodeTbl = new HashMap<>();
        String line = null;
        
        try {
            BufferedReader reader = new BufferedReader(new FileReader(EDGES));
            
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                
                if (line.isEmpty()) {
                    break;
                }
                
                String[] edge = line.split(DELIMITER);
                long srcId = Long.parseLong(edge[0]);
                long dstId = Long.parseLong(edge[1]);
                double x = Double.parseDouble(edge[2]);
                
                if (!nodeTbl.containsKey(srcId)) {
                    nodeTbl.put(srcId, new Node(srcId));
                }
                
                if (!nodeTbl.containsKey(dstId)) {
                    nodeTbl.put(dstId, new Node(dstId));
                }
                
                if (REJECT_MIN <= x && x < REJECT_MAX) {
                    continue;
                }
                
                nodeTbl.get(srcId).addDestination(dstId);
            }
            
            reader.close();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return nodeTbl;
    }
    
    /**
     * Filters out all the Edges valued within REJECT_MIN and REJECT_MAX.
     * Writes the valid Edges into another file.
     */
    public static void filterEdges() {
        String line = null;
        long currSrcId = -1;
        
        try {
            BufferedReader reader = new BufferedReader(new FileReader(EDGES));
            BufferedWriter writer = new BufferedWriter(new FileWriter(FLTRED, false));
            
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                
                if (line.isEmpty()) {
                    break;
                }
                
                String[] edge = line.split(DELIMITER);
                long srcId = Long.parseLong(edge[0]);
                long dstId = Long.parseLong(edge[1]);
                double x = Double.parseDouble(edge[2]);
                
                if (srcId != currSrcId) {
                    if (currSrcId != -1) {
                        writer.newLine();
                        
                        while (currSrcId + 1 != srcId) {
                            currSrcId ++;
                            writer.write(Long.toString(currSrcId));
                            writer.write(SPACE);
                            writer.write(Double.toString(INIT_PR));
                            writer.newLine();
                        }
                    }
                    
                    writer.write(Long.toString(srcId));
                    writer.write(SPACE);
                    writer.write(Double.toString(INIT_PR));
                    currSrcId = srcId;
                }
                
                if (REJECT_MIN <= x && x < REJECT_MAX) {
                    continue;
                }
                
                writer.write(SPACE);
                writer.write(Long.toString(dstId));
            }
            
            reader.close();
            writer.close();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * Processes the filtered Edges. Returns all the Nodes in a map.
     * @return map of all Nodes
     */
    public static Map<Long, Node> processFilteredEdges() {
        Map<Long, Node> nodeTbl = new HashMap<>();
        
        try {
            String line = null;
            BufferedReader reader = new BufferedReader(new FileReader(FLTRED));
            
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                
                if (line.isEmpty())
                    break;
                
                String[] node = line.split(DELIMITER, 3);
                long srcId = Long.parseLong(node[0]);
                
                if (!nodeTbl.containsKey(srcId)) {
                    nodeTbl.put(srcId, new Node(srcId));
                }
                
                Node srcNode = nodeTbl.get(srcId);
                
                if (node.length < 3) {  // no out-going edge
                    continue;
                }
                
                String[] dstIds = node[2].trim().split(DELIMITER);
                
                for (int i = 0; i < dstIds.length; i ++) {
                    long dstId = Long.parseLong(dstIds[i]);
                    srcNode.addDestination(dstId);
                    
                    if (!nodeTbl.containsKey(dstId)) {
                        nodeTbl.put(dstId, new Node(dstId));
                    }
                }
            }
            
            reader.close();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return nodeTbl;
    }
}
