package FPGrowthRuleMining;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper.Context;


//Static methods that provide utility of translating Strings into itemsets, etc
//DXXSHA001
//25 July 2018

public class ItemsetUtils {
	
	/**
	 * Reads a string representation of an itemset and returns the itemset object.
	 * String representation defined in the toString() method of the Itemset class.
	 * @param String representation of an itemset object
	 * @return Itemset object
	 */
	public static Itemset readItemset(String items) {
		Itemset itemset = new Itemset();
		items = items.replace("[", "");
		items = items.replace("]", "");
		
		String[] itemsArr = items.split(", ");
		for (int i = 0; i < itemsArr.length; i++) {
			itemset.addItem(itemsArr[i]);
		}
		return itemset;
	}
	
	/**
	 * Returns the list of all itemsets in the output provided in the map reduce output.
	 * @param conf
	 * @param outputDir
	 * @return
	 */
	public static List<Itemset> getItem1Sets(Configuration conf, String outputDir) {
		List<Itemset> itemsets = new ArrayList<Itemset>();
		
		String file = outputDir + "/part-r-00000";
		
		//Read file
		try {
			Path path =  new Path(file);
			FileSystem fs = FileSystem.get(conf);
			Scanner scOutput = new Scanner(fs.open(path));
			while(scOutput.hasNextLine()) {
				String[] keyValue = scOutput.nextLine().split("\t");
				Itemset freq1Itemset = readItemset(keyValue[0]);
				freq1Itemset.setSupport(Integer.parseInt(keyValue[1]));
				itemsets.add(freq1Itemset);
			}
			scOutput.close();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
		return itemsets;
	}
	
	/**
	 * Uses the C[k] = F[k-1] x F[1] method described in https://chih-ling-hsu.github.io/2017/03/25/apriori
	 * This method makes use of the apriori principle: if an itemset if frequent, all its subsets are frequent. 
	 * Conversely, if an itemset is infrequent, all its supersets will also be infrequent.
	 * This method only uses this principle by using only frequent itemsets.
	 * @param List of all the frequent (k-1) itemsets
	 * @param Prefix of the output directory that previous frequent itemsets were written to, used to find frequent-1-itemsets
	 * @return 
	 */
	@SuppressWarnings("rawtypes")
	public static List<Itemset> genCandidateKItemsets(List<Itemset> prevFreqItemsets, Context context){
		//Variables
		List<Itemset> candidates        = new ArrayList<Itemset>();
		List<Itemset> frequent1Itemsets = new ArrayList<Itemset>();
		
		//Get the frequent-1-itemsets
		String frequent1ItemsetsDir = context.getConfiguration().get("fs.default.name") 
				+ context.getConfiguration().get("outputDirPrefix") 
				+ "1" 
				+ "/part-r-00000";
		
		//Read file
		try {
			Path path =  new Path(frequent1ItemsetsDir);
			FileSystem fs = FileSystem.get(context.getConfiguration());
			Scanner scOutput = new Scanner(fs.open(path));
			while(scOutput.hasNextLine()) {
				String[] keyValue = scOutput.nextLine().split("\t");
				Itemset freq1Itemset = readItemset(keyValue[0]);
				freq1Itemset.setSupport(Integer.parseInt(keyValue[1]));
				frequent1Itemsets.add(freq1Itemset);
			}
			scOutput.close();
		}
		catch(Exception e) {
			e.printStackTrace(System.out);
		}
		
		//Combine F[k-1] with F[1] - only combine if a not contains b
		for(int i=0; i<prevFreqItemsets.size(); i++) {
			for(int j=0; j<frequent1Itemsets.size(); j++) {
				if(!frequent1Itemsets.get(j).isSubset(prevFreqItemsets.get(i))) {
					//might have problems
					Itemset candidate = new Itemset(prevFreqItemsets.get(i));
					candidate.addSet(frequent1Itemsets.get(j));
					candidates.add(candidate);
				}
			}
		}
		
		//Output
		return candidates;
	}
	
	/**
	 * Finds the index of the item in the frequent pattern.
	 * @param All items in the frequent pattern seperated by ','.
	 * @param The specified item that you wish to find the index of.
	 * @return The index of the specified item in the frequent pattern.
	 */
	public static int findFreqPatternIndex(String freqPattern, String item) {
		String[] freqPatternList = freqPattern.split(",");
		for(int i=0; i<freqPatternList.length; i++) {
			if(freqPatternList[i].equals(item)) {
				return i;
			}
		}
		return -1;
	}
	
	public static void printNodeLinkTable(FPTreeNode[] table) {
		for(FPTreeNode n : table) {
			FPTreeNode current = n;
			if(current == null) {
				System.out.println("null");
			}
			else {
				current.printValue(); System.out.print(" -> ");
				if(!current.hasNext()) {
					System.out.print("null\n");
				}
				while(current.hasNext()) {
					current = current.getNext();
					current.printValue(); System.out.print(" -> ");
				}
				System.out.print("null\n");
			}
			
		}
	}
	
	/**
	 * Finds the longest matching prefix wrt items in itemsets, this is wrong
	 * https://codereview.stackexchange.com/questions/46965/longest-common-prefix-in-an-array-of-strings
	 * @param condPattBaseList
	 * @return
	 */
	public static int findConditionalFPTreeIndex(List<Itemset> condPattBaseList) {
		for(int itemIndex=0; itemIndex < condPattBaseList.get(0).size(); itemIndex++) {
			String item = condPattBaseList.get(0).getItemset().get(itemIndex);
			//Compare item with each conditional pattern base that isnt the first one
			for(int pattern=1; pattern<condPattBaseList.size(); pattern++) {
				if(itemIndex >= condPattBaseList.get(pattern).size() || 
						!(condPattBaseList.get(pattern).getItemset().get(itemIndex).equals(item))) {
					//Mismatch
					return itemIndex;
				}
			}
		}
		return condPattBaseList.get(0).size();
	}
	
	//wrong
	public static FPTreeNode constructCondFPTree(List<Itemset> condPattBaseList) {
		FPTreeNode condFPTree = new FPTreeNode();
		FPTreeNode current = condFPTree;
		for(Itemset itemset : condPattBaseList) {
			current = condFPTree;
			for(String item : itemset.getItemset()) {
				FPTreeNode child = current.addChild(item, itemset.getSupport());
				current = child;
			}
		}
		
		return condFPTree;
	}
	
	
	
	/**
	 * Adds frequent itemsets to the list given, recursively. works
	 * @param freqItemsetList - List of frequent itemsets generated. Alg will add to this list. Pass in an empty list.
	 * @param condFPTree
	 * @param freqItemset
	 * @param minSupport
	 * @param 
	 */
	public static void mineFreqItemset(List<Itemset> freqItemsetList, List<FPTreeNode> condFPTree, String freqItemsetString, int index, int count){
		if(condFPTree.size() == index) {
			return;
		}
		
		for(int i=index; i<condFPTree.size(); i++) {
			Itemset freqItemset = readItemset(freqItemsetString);
			int minCount = Math.min(count, condFPTree.get(i).getCount());
			freqItemset.addItem(condFPTree.get(i).getItem());
			freqItemset.setSupport(minCount);
			freqItemsetList.add(freqItemset);
			mineFreqItemset(freqItemsetList, condFPTree, freqItemset.toString(), (index+(i+1)), count);
			
		}
	}
	
	public static List<Itemset> readFreqItemsets(String line){
		List<Itemset> itemsets = new ArrayList<Itemset>();
		String[] itemsetsLine = line.split(";");
		for(String s : itemsetsLine) {
			if(!s.equals("")) {
				String[] keyVal = s.split(":");
				Itemset i = readItemset(keyVal[0]);
				i.setSupport(Integer.parseInt(keyVal[1]));
				itemsets.add(i);
			}
		}
		return itemsets;
	}
	
	public static Itemset findItemset(List<Itemset> itemsets, Itemset itemset) {
		for(Itemset i : itemsets) {
			if(i.equals(itemset)) {
				return i;
			}
		}
		return null;
	}
	
	//gen rules for i, given the list of freq itemsets in a line
	public static void genAssocRules(List<AssociationRule> rules, List<Itemset> freqItemsets, List<Itemset> freq1sets, Itemset itemset){
		int size = itemset.size();
		
		Itemset first = new Itemset();
		for(int i=0; i<(size-1); i++) {
			first.addItem(itemset.getItemset().get(i));
		}
		Itemset second = new Itemset();
		second.addItem(itemset.getLastItem());
		if(first.size()==1) {
			first = findItemset(freq1sets, first);
		}
		else {
			first = findItemset(freqItemsets, first);
		}
		second = findItemset(freq1sets, second);
		
		rules.add(new AssociationRule(first, second, itemset.getSupport()));
		rules.add(new AssociationRule(second, first, itemset.getSupport()));
	}
	
	/**
	 * Also prunes infrequent items
	 * @param condPattBase
	 * @param support
	 * @return List of items that are frequent. In the form of a list of itemsets where each itemset only contains 1 item.
	 */
	public static List<Itemset> findFList(List<Itemset> condPattBase, int support){
		FPTreeNode fList = new FPTreeNode();
		for(Itemset itemset : condPattBase) {
			for(String item : itemset.getItemset()) {
				fList.addChild(item, itemset.getSupport());
			}
		}
		List<Itemset> freqList = new ArrayList<Itemset>();
		for(FPTreeNode n : fList.getChildrenNodes()) {
			if(n.getCount() >= support) {
				Itemset i = new Itemset();
				i.addItem(n.getItem());
				i.setSupport(n.getCount());
				freqList.add(i);
			}
		}
		return freqList;
	}
	
	/*
	public static boolean isSinglePath(FPTreeNode node) {
		FPTreeNode current = node;
		while(current.hasChildren()) {
			if(current.getChildrenNodes().size() > 1) {
				return false;
			}
			else {
				current = current.getChildrenNodes().get(0);
			}
		}
		return true;
	}//*/
	
	/**
	 * Constructs the cond pattern base for items up to a certain point in the freq pattern
	 * feed in cond patt base and until where it should stop in the freq pattern
	 */
	public static List<Itemset> constructCondPattBase(List<Itemset> origCondPattBase, Itemset[] frequentPattern, String endItem) {
		List<Itemset> condPattBase = new ArrayList<Itemset>();
		
		for(Itemset i : origCondPattBase) {
			if(!i.contains(endItem)) {
				continue;
			}
			Itemset condItemset = new Itemset();
			
			for(Itemset patt : frequentPattern) {
				String item = patt.getFirstItem();
				if(item.equals(endItem)) {
					break;
				}
				else if(i.contains(item)) {
					condItemset.addItem(item);
				}
			}
			
			if(condItemset.size()!=0) {
				condItemset.setSupport(i.getSupport());
				condPattBase.add(condItemset);
			}
			
		}
		return condPattBase;
	}
	
	/**
	 * Constructs frequent itemsets using the conditional FP trees of further itemsets
	 * @param frequentItemsets
	 * @param freqPatternList
	 * @param origCondPattBase
	 * @param base
	 * @param minSupport
	 */
	public static void constructFreqItemsets(List<Itemset> frequentItemsets, Itemset[] freqPatternList, List<Itemset> origCondPattBase, Itemset base, int minSupport) {
		List<Itemset> condPattBase = constructCondPattBase(origCondPattBase, freqPatternList, base.getFirstItem());
		/*TEST
		System.out.println("cond patt base for end item "+base.getFirstItem());
		for(Itemset i : condPattBase) {
			System.out.println(i.toString()+": "+i.getSupport());
		}//*/
		if(condPattBase.isEmpty()) {
			return;
		}
		List<Itemset> fList = ItemsetUtils.findFList(condPattBase, minSupport);
		for(Itemset i : fList) {
			Itemset freqItemset = new Itemset(base);
			String condItem = i.getLastItem();
			freqItemset.addItemToFront(condItem);
			freqItemset.setSupport(Math.min(base.getSupport(), i.getSupport()));
			frequentItemsets.add(freqItemset);
			constructFreqItemsets(frequentItemsets, freqPatternList, condPattBase, freqItemset, minSupport);
		}
	}
}
