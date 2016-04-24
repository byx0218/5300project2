package pagerank;

import java.util.Map;

public class PageRank {
    
    public static void main(String[] args) {
        EdgeFilter filter = new EdgeFilter();
        filter.filterEdges();
        
        // test parser OK
//        Map<Long, Node> nodeTbl1 = filter.processEdges();
//        Map<Long, Node> nodeTbl2 = filter.processFilteredEdges();
//        
//        System.out.println(nodeTbl1.size());
//        System.out.println(nodeTbl2.size());
//        System.out.println();
//        
//        for (Long srcId : nodeTbl1.keySet()) {
//            Node n1 = nodeTbl1.get(srcId);
//            
//            if (nodeTbl2.containsKey(srcId)) {
//                Node n2 = nodeTbl2.get(srcId);
//                
//                if (!n1.getDestinationList().equals(n2.getDestinationList())) {
//                    System.out.println("node error: " + srcId);
//                    break;
//                }
//            } else {
//                System.out.println("key error: " + srcId);
//                break;
//            }
//        }
//        
//        System.out.println("done");
    }
    
}
