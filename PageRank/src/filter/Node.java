package filter;

import java.util.ArrayList;
import java.util.List;

public class Node {
    
    /** ID of this node. */
    private long nodeId;
    
    /** Page rank of this node. */
    private float pageRank;
    
    /** Neighbors (destinations) of this node. */
    private List<Long> dest;
    
    /**
     * Constructs a Node with given Node ID.
     * @param id Node ID
     */
    public Node(long id) {
        this(id, 0.0f);
    }
    
    /**
     * Constructs a Node with given Node ID and its page rank.
     * @param id Node ID
     * @param pr Node's page rank
     */
    public Node(long id, float pr) {
        nodeId = id;
        pageRank = pr;
        dest = new ArrayList<>();
    }
    
    /**
     * Returns the ID of this Node.
     * @return Node ID
     */
    public long getNodeId() {
        return nodeId;
    }
    
    /**
     * Returns the page rank of this Node.
     * @return page rank of this Node
     */
    public float getPageRank() {
        return pageRank;
    }
    
    /**
     * Sets the page rank of this Node to the given float value.
     * @param pr page rank value for this Node to set to
     */
    public void setPageRank(float pr) {
        pageRank = pr;
    }
    
    /**
     * Adds a destination Node ID to the destination list of this Node.
     * @param id destination Node ID
     */
    public void addDestination(long id) {
        dest.add(id);
    }
    
    /**
     * Returns the destination Node list of this Node.
     * @return destination Node list of this Node
     */
    public List<Long> getDestinationList() {
        return dest;
    }
    
}
