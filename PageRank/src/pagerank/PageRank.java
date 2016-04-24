package pagerank;

import java.util.Map;

public class PageRank {
	
	public static void main(String[] args) {
		EdgeFilter filter = new EdgeFilter();
		filter.filterEdges();
		
		Map<Long, Node> nodeTbl1 = filter.processEdges();
		Map<Long, Node> nodeTbl2 = filter.processEdges();
		
	}
	
}
