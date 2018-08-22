package FPGrowthRuleMining;

import java.util.List;
import java.util.ArrayList;

public class FPTreeNode {
	//Variables
	private String item;
	private int count;
	private FPTreeNode next;
	private FPTreeNode parent;
	private List<FPTreeNode> children;
	
	//Constructor
	public FPTreeNode() {
		this.item = null;
		this.count = 0;
		this.next = null;
		this.parent = null;
		this.children = new ArrayList<FPTreeNode>();
	}

	public FPTreeNode(String item, int count) {
		this.item = item;
		this.count = count;
		this.next = null;
		this.parent = null;
		this.children = new ArrayList<FPTreeNode>();
	}
	
	//Copy constructor
	public FPTreeNode(FPTreeNode other) {
		this.item = other.item;
		this.count = other.count;
		this.next = null;
		this.parent = null;
		this.children = new ArrayList<FPTreeNode>();
	}
	
	//Methods
	/**
	 * Returns the item that the node represents.
	 * @return Item associated with the node.
	 */
	public String getItem() {
		return this.item;
	}
	
	/**
	 * Returns the count of this node.
	 * @return Count of the node.
	 */
	public int getCount() {
		return this.count;
	}
	
	/**
	 * Sets the next node.
	 * @param next Node to link to this
	 */
	public void setNext(FPTreeNode next) {
		this.next = next;
	}
	
	/**
	 * Returns the next node that this is linked to.
	 * @return Next node.
	 */
	public FPTreeNode getNext() {
		return this.next;
	}
	
	/**
	 * Determines if this node is linked to another node.
	 * @return The next node.
	 */
	public boolean hasNext() {
		return this.next != null;
	}
	
	/**
	 * Sets this node's parent ot the node specified.
	 * @param parent Parent node.
	 */
	public void setParent(FPTreeNode parent) {
		this.parent = parent;
	}
	
	/**
	 * Returns the parent of this node.
	 * @return Parent node.
	 */
	public FPTreeNode getParent() {
		return this.parent;
	}
	
	/**
	 * Determines if this node has a parent node or not.
	 * @return true if this has a parent, false if not.
	 */
	public boolean hasParent() {
		return this.parent != null;
	}
	
	/**
	 * Returns the list of nodes that are children of this.
	 * @return List of children nodes.
	 */
	public List<FPTreeNode> getChildrenNodes(){
		return this.children;
	}
	
	/**
	 * Adds a new child node to this node if it does not exist yet.
	 * If this node has a child with the same item, it increases the count of that child.
	 * @param item Item to add.
	 * @param count Count of the item.
	 * @return The child FPNode that was added.
	 */
	public FPTreeNode addChild(String item, int count) {
		int childIndex = this.hasChild(item);
		if(childIndex == -1) {
			FPTreeNode child = new FPTreeNode(item, count);
			this.children.add(child);
			child.setParent(this);
			return child;
		}
		else {
			this.children.get(childIndex).addCount(count);
			return this.children.get(childIndex);
		}
	}
	
	/**
	 * Adds to the count of this node.
	 * @param count Amount to increment.
	 */
	public void addCount(int count) {
		this.count += count;
	}
	
	/**
	 * Determines if this node has a child node whose item is the one specified. 
	 * @param item Item to check.
	 * @return The index of the child of this node. Returns -1 if this node does not have a child with the item specified.
	 */
	public int hasChild(String item) {
		for(int i=0; i<this.children.size(); i++) {
			if(this.children.get(i).getItem().equals(item)) {
				return i;
			}
		}
		return -1;
	}
	
	/**
	 * Determines if this node has children nodes.
	 * @return true if this has children nodes, false if not.
	 */
	public boolean hasChildren() {
		return this.children.size() != 0;
	}
	
	/**
	 * Prints the tree from the node given as the root of the tree.
	 * @param root Root of the node to print from
	 * @param level Level to start with (0)
	 */
	public static void testPrintTree(FPTreeNode root, int level) {
		System.out.print(stringBuilder(level));
		System.out.print("_______");
        root.printValue();
        System.out.println();
        int lvl = level;
        if (!root.children.isEmpty()){
            lvl++;
            for (FPTreeNode n: root.children){
                testPrintTree(n, lvl);
            }
        }
	}
	private static String stringBuilder(int level){
        String s = "";
        for (int i=0; i<level; i++){
            if(level>0){
                s+="      |";
            }
            else{
                s+="       ";
            }
        }
        return s;
	}
	
	public void printValue() {
		System.out.print("\""+this.item+"\""+":"+this.count);
	}
}
