package util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

public class EdgeFilter {
    
    /** Files to read or write. */
    private static final String EDGES = "edges.txt";
    private static final String BLOCKS = "blocks.txt";
    private static final String FLTRED = "filtered_edges.txt";
    
    /** Filter parameters from NetID "xg95". */
    private static final double NET_ID = 0.59;
    private static final double REJECT_MIN = 0.9 * NET_ID;
    private static final double REJECT_MAX = REJECT_MIN + 0.01;
    
    /** Initial page rank value for all nodes. */
    private static final double INIT_PR = 1.0 / util.Const.N;
    
//    public static void main(String[] args) {
//        filterEdges();
//        int numEdges = countEdges();
//        
//        System.out.println("Reject min: " + REJECT_MIN);
//        System.out.println("Reject max: " + REJECT_MAX);
//        
//        System.out.println("Number of edges selected: " + numEdges);
//    }
    
    /**
     * Processes the blocks.txt file.
     */
    public static int[] processBlocks() {
        String line = null;
        List<Integer> blocks = new ArrayList<Integer>();
        
        try {
            BufferedReader reader = new BufferedReader(new FileReader(BLOCKS));
            
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                
                if (line.isEmpty()) {
                    break;
                }
                
                blocks.add(Integer.parseInt(line));
            }
            
            reader.close();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        int nodeId = 0;
        int[] blockIds = new int[blocks.size()];
        
        for (int i = 0; i < blocks.size(); i ++) {
            nodeId += blocks.get(i);
            blockIds[i] = nodeId;
        }
        
        return blockIds;
    }

    
    /**
     * Filters out all the Edges valued within REJECT_MIN and REJECT_MAX.
     * Writes the selected Edges into another file.
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
                
                String[] edge = line.split(util.Const.DELIMITER);
                long srcId = Long.parseLong(edge[0]);
                long dstId = Long.parseLong(edge[1]);
                double x = Double.parseDouble(edge[2]);
                
                if (srcId != currSrcId) {
                    if (currSrcId != -1) {
                        writer.newLine();
                        
                        while (currSrcId + 1 != srcId) {
                            currSrcId ++;
                            writer.write(Long.toString(currSrcId));
                            writer.write(util.Const.SPACE);
                            writer.write(Double.toString(INIT_PR));
                            writer.newLine();
                        }
                    }
                    
                    writer.write(Long.toString(srcId));
                    writer.write(util.Const.SPACE);
                    writer.write(Double.toString(INIT_PR));
                    currSrcId = srcId;
                }
                
                if (REJECT_MIN <= x && x < REJECT_MAX) {
                    continue;
                }
                
                writer.write(util.Const.SPACE);
                writer.write(Long.toString(dstId));
            }
            
            reader.close();
            writer.close();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    
    /**
     * Counts the number of selected edges in filtered_edges.txt file.
     * @return the number of selected edges
     */
    public static int countEdges() {
        String line = null;
        int count = 0;
        
        try {
            BufferedReader reader = new BufferedReader(new FileReader(FLTRED));
            
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                
                if (line.isEmpty()) {
                    break;
                }
                
                String[] node = line.split(util.Const.DELIMITER);
                count += node.length - 2;
            }
            
            reader.close();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return count;
    }
    
}
