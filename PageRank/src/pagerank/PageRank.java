package pagerank;

import java.util.Map;

public class PageRank {
	
	public static void main(String[] args) {
		EdgeFilter filter = new EdgeFilter();
		Map<String, Node> nodeTbl = filter.processEdges();
		filter.close();
		
		System.out.println(nodeTbl.size());
	}
	
	
}
