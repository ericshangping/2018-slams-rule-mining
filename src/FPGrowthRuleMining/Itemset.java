//A set of topics and it's count
//DXXSHA001
//16 Jul 2018

package FPGrowthRuleMining;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import com.google.common.collect.Lists;

public class Itemset implements Comparable<Itemset>{
	//Variables
	private List<String> itemset;
	private int supportCount;
	
	//Constructors
	public Itemset() {
		itemset = new ArrayList<String>();
		this.supportCount = -1;
	}
	
	//Copy constructor
	public Itemset(Itemset i) {
		this.itemset = new ArrayList<String>(i.itemset);
		this.supportCount = i.supportCount;
	}

	//Methods
	/**
	 * Returns the number of items are in the set.
	 * @return
	 */
	public int size() {
		return itemset.size();
	}
	
	/**
	 * Returns the underlying itemset.
	 * @return List of Strings
	 */
	public List<String> getItemset(){
		return this.itemset;
	}
	
	/**
	 * Returns the first item in the underlying itemset.
	 * @return First item in the list.
	 */
	public String getFirstItem() {
		return this.itemset.get(0);
	}
	
	/**
	 * Returns the first item in the underlying itemset.
	 * @return Last item in the list.
	 */
	public String getLastItem() {
		return this.itemset.get(this.itemset.size()-1);
	}
	
	/**
	 * Sets the support count of this itemset.
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
	
	/**
	 * Increments support count
	 */
	public void incSupport() {
		this.supportCount++;
	}
	
	/**
	 * Adds a string to the set.
	 * @param i Item to add to the itemset.
	 */
	public void addItem(String i) {
		itemset.add(i);
	}
	
	/**
	 * Adds item to the back of the set.
	 * @param i Item to add
	 */
	public void addItemToFront(String i) {
		this.itemset.add(0, i);
	}
	
	/**
	 * Adds all elements of the given itemset to this itemset.
	 * @param itemset Itemset to add to this.
	 * @deprecated
	 */
	public void addSet(Itemset itemset) {
		for(String i : itemset.itemset) {
			this.itemset.add(i);
		}
	}
	
	/**
	 * Removes the last item in the itemset
	 */
	public void removeLastItem() {
		this.itemset.remove(this.itemset.size()-1);
	}
	
	/**
	 * Only used for comparing results
	 */
	public void sortItemset() {
		String[] sortedList = new String[this.itemset.size()];
		this.itemset.toArray(sortedList);
		Arrays.sort(sortedList);
		this.itemset = new ArrayList<String>();
		for(String s : sortedList) {
			this.itemset.add(s);
		}
	}
	
	/**
	 * Reverses the order of the underlying itemset.
	 * @deprecated
	 */
	public void reverseSet() {
		this.itemset = Lists.reverse(this.itemset);
	}
	
	/**
	 * Determines if the underlying itemset contains an item.
	 * @param item The item to check.
	 * @return true if the List contains the item, false if not.
	 */
	public boolean contains(String item) {
		for(String i : this.itemset) {
			if(i.equals(item)) {
				return true;
			}
		}
		return false;
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
	 * Checks for equality of the underlying sets.
	 * @param i Itemset to compare.
	 * @return true if the subset of this itemset equals the subset of the other. false if not.
	 */
	public boolean equals(Itemset i) {
		return this.isSubset(i) && i.isSubset(this);
	}

	@Override
	/**
	 * Compares two itemsets by support count.
	 */
	public int compareTo(Itemset i) {
		return new Integer(i.supportCount).compareTo(new Integer(this.supportCount));
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
}
