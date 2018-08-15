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
	
	public FPTreeNode(FPTreeNode other) {
		this.item = other.item;
		this.count = other.count;
		this.next = null;
		this.parent = null;
		this.children = new ArrayList<FPTreeNode>();
	}
	
	//Methods
	public String getItem() {
		return this.item;
	}
	
	public int getCount() {
		return this.count;
	}
	
	public void setNext(FPTreeNode next) {
		this.next = next;
	}
	
	public FPTreeNode getNext() {
		return this.next;
	}
	
	public boolean hasNext() {
		return this.next != null;
	}
	
	public void setParent(FPTreeNode parent) {
		this.parent = parent;
	}
	
	public FPTreeNode getParent() {
		return this.parent;
	}
	
	public boolean hasParent() {
		return this.parent != null;
	}
	
	public List<FPTreeNode> getChildrenNodes(){
		return this.children;
	}
	
	/**
	 * Adds a new child node to this node if it does not exist yet.
	 * If this node has a child with the same item, it increases the count of that child.
	 * @param item
	 * @param count
	 * @return Returns the child
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
	
	/*
	public FPTreeNode addCondChild(String item, int count) {
		int childIndex = this.hasChild(item);
		if(childIndex == -1) {
			FPTreeNode child = new FPTreeNode(item, count);
			this.children.add(child);
			child.setParent(this);
			return child;
		}
		else {
			//just dont add counts
			//this.children.get(childIndex).addCount(count);
			return this.children.get(childIndex);
		}
	}//*/
	
	/*
	public void addChild(FPTreeNode child) {
		this.children.add(child);
		child.setParent(this);
	}//*/
	
	public void addCount(int count) {
		this.count += count;
	}
	
	/**
	 * Determines if this node has a child node whose item is the one specified. 
	 * @param item
	 * @return The index of the child of this node 
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
	 * Prints the tree from the node given as the root of the tree.
	 * @param Root of the node to print from
	 * @param Level to start with (1)
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
