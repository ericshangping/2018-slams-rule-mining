package FPGrowthRuleMining;
//A set of topics and it's count
//DXXSHA001
//16 Jul 2018

import java.util.List;
import java.util.ArrayList;
//import java.util.Set;
//import java.util.TreeSet;

import org.apache.hadoop.io.Text;

import com.google.common.collect.Lists;

public class Itemset implements Comparable<Itemset>{
	//Variables
	private Text ID;
	private List<String> itemset;
	private int supportCount;
	
	//Constructors
	public Itemset() {
		ID = new Text();
		itemset = new ArrayList<String>();
		this.supportCount = -1;
	}
	
	public Itemset(String ID) {
		this.ID = new Text(ID);
		itemset = new ArrayList<String>();
		this.supportCount = -1;
	}
	
	//Copy constructor
	public Itemset(Itemset i) {
		this.ID = new Text(i.ID);
		this.itemset = new ArrayList<String>(i.itemset);
		this.supportCount = i.supportCount;
	}

	//Methods
	/**
	 * Adds a string to the set.
	 * @param i
	 */
	public void addItem(String i) {
		itemset.add(i);
	}
	
	/**
	 * Adds all elements of the given itemset to this itemset.
	 * @param itemset
	 */
	public void addSet(Itemset itemset) {
		for(String i : itemset.itemset) {
			this.itemset.add(i);
		}
	}
	
	/**
	 * Checks if this itemset is a subset of the given itemset.
	 * @param itemset
	 * @return
	 */
	public boolean isSubset(Itemset itemset) {
		return itemset.itemset.containsAll(this.itemset);
	}
	
	/**
	 * Returns the number of items are in the set.
	 * @return
	 */
	public int size() {
		return itemset.size();
	}
	
	@Override
	/**
	 * String representation of the itemset: [0, 1, 2, ..., n]
	 */
	public String toString() {
		String out = "[";
		if(this.itemset.isEmpty()) {
			out += "]";
			return out;
		}
		for (String i : itemset) {
			out += i + ", ";
		}
		out = out.substring(0, out.length()-2);
		out += "]";
		return out;
	}
	
	/**
	 * Sets the support count of this itemset
	 * @param support
	 */
	public void setSupport(int support) {
		this.supportCount = support;
	}
	
	/**
	 * Returns the support count of this itemset
	 * @return supportCount
	 */
	public int getSupport() {
		return this.supportCount;
	}
	
	public List<String> getItemset(){
		return this.itemset;
	}
	
	public void reverseSet() {
		this.itemset = Lists.reverse(this.itemset);
	}
	
	public boolean contains(String item) {
		for(String i : this.itemset) {
			if(i.equals(item)) {
				return true;
			}
		}
		return false;
	}
	
	public String getLastItem() {
		return this.itemset.get(this.itemset.size()-1);
	}
	
	/**
	 * Checks for equality of the underlying sets.
	 * @param i
	 * @return
	 */
	public boolean equals(Itemset i) {
		return this.isSubset(i) && i.isSubset(this);
	}

	@Override
	public int compareTo(Itemset i) {
		return new Integer(i.supportCount).compareTo(new Integer(this.supportCount));
	}
}
