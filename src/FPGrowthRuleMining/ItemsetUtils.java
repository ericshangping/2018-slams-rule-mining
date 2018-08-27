//Static methods that provide utility methods to help the generation of frequent itemsets and association rules
//DXXSHA001
//25 July 2018

package FPGrowthRuleMining;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.IOException;

import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;

public class ItemsetUtils {
	
	/**
	 * Reads a string representation of an itemset and returns the itemset object.
	 * String representation defined in the toString() method of the Itemset class.
	 * @param items String representation of an itemset object
	 * @return Itemset object read in
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
	 * Returns the list of all frequent items from the output directory of the first map reduce job.
	 * @param conf Hadoop configuration variable being used
	 * @param outputDir Directory of where the frequent items were written to
	 * @return List of frequent items as Itemsets with one item
	 * @throws IOException 
	 */
	public static List<Itemset> getFreqItems(Configuration conf, String outputDir) throws IOException {
		List<Itemset> itemsets = new ArrayList<Itemset>();
		FileSystem fs = FileSystem.get(conf);
		String base = outputDir + "/part-r-";
		int part = 0;
		
		String partString = ""+part;
		String file = base + (("00000"+(partString)).substring(partString.length()));
		Path path =  new Path(file);
		//Read all files
		do {
			//Read itemsets
			Scanner scOutput = new Scanner(fs.open(path));
			while(scOutput.hasNextLine()) {
				String[] keyValue = scOutput.nextLine().split("\t");
				Itemset freq1Itemset = readItemset(keyValue[0]);
				freq1Itemset.setSupport(Integer.parseInt(keyValue[1]));
				itemsets.add(freq1Itemset);
			}
			scOutput.close();
			
			//Next file
			part++;
			partString = ""+part;
			file = base + (("00000"+(partString)).substring(partString.length()));
			path =  new Path(file);
		}while(fs.exists(path));

		return itemsets;
	}
	
	/**
	 * Uses the C[k] = F[k-1] x F[1] method described in https://chih-ling-hsu.github.io/2017/03/25/apriori
	 * This method makes use of the apriori principle: if an itemset if frequent, all its subsets are frequent. 
	 * Conversely, if an itemset is infrequent, all its supersets will also be infrequent.
	 * This method only uses this principle by using only frequent itemsets.
	 * @deprecated This method is inefficient.
	 * @param prevFreqItemsets List of all the frequent (k-1) itemsets
	 * @param context Prefix of the output directory that previous frequent itemsets were written to, used to find frequent-1-itemsets
	 * @return List of frequent itemsets.
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
	
	/**
	 * Reads a line of frequent itemsets found with a base item. 
	 * @param line String containing all frequent itemsets.
	 * @return List of frequent itemsets.
	 */
	public static List<Itemset> readFreqItemsets(String line){
		List<Itemset> itemsets = new ArrayList<Itemset>();
		String[] itemsetsLine = line.split(";");
		for(String s : itemsetsLine) {
			if(!s.equals("\t")) {
				String[] keyVal = s.split(":");
				Itemset i = readItemset(keyVal[0]);
				i.setSupport(Integer.parseInt(keyVal[1]));
				itemsets.add(i);
			}
		}
		return itemsets;
	}
	
	/**
	 * Finds an itemset in a list of itemsets given the items that it should contain. This is to use the support count.
	 * @param itemsets List of itemsets to search.
	 * @param itemset Itemset containing the items specifying the itemset to find.
	 * @return The itemset if it found it. Null if it wasn't found.
	 */
	public static Itemset findItemset(List<Itemset> itemsets, Itemset itemset) {
		for(Itemset i : itemsets) {
			if(i.equals(itemset)) {
				return i;
			}
		}
		return null;
	}
	
	/**
	 * Finds the list of items that are frequent in the given conditional pattern base.
	 * @param condPattBase Conditional pattern base of the itemset.
	 * @param support Minimum support level.
	 * @return List of items that are frequent. In the form of a list of itemsets, where each itemset only contains 1 item.
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
	
	/**
	 * Constructs the cond pattern base for items up to a certain point in the freq pattern.
	 * @param origCondPattBase The original conditional pattern base.
	 * @param frequentPattern The frequent pattern.
	 * @param endItem An item in the frequent pattern where the new conditional pattern base will stop at.
	 * @return List containing the new conditional pattern base.
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
	 * Constructs frequent itemsets by using an itemset's conditional pattern base 
	 * and finding the conditional FP trees for those itemsets.
	 * This is called recursively until all frequent itemsets are found for the item.
	 * @param frequentItemsets List of frequent itemsets that are found.
	 * @param freqPatternList The frequent pattern used for the construction of conditional pattern bases.
	 * @param origCondPattBase Original conditional pattern base of the itemset.
	 * @param base Base itemset to generate frequent itemsets from.
	 * @param minSupport Minimum support level.
	 */
	public static void constructFreqItemsets(List<Itemset> frequentItemsets, Itemset[] freqPatternList, List<Itemset> origCondPattBase, Itemset base, int minSupport) {
		List<Itemset> condPattBase = constructCondPattBase(origCondPattBase, freqPatternList, base.getFirstItem());
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
	
	/**
	 * Generates combinations of association rules. Uses the pattern of generation of frequent itemsets.
	 * @param rules List of association rules to add to.
	 * @param freqItemsets List of frequent itemsets to look up support counts.
	 * @param freqItems List of frequent items, since they are not included in the freqItemsets list.
	 * @param itemset Itemset to generate association rules for.
	 */
	public static void genAssocRules(List<AssociationRule> rules, List<Itemset> freqItemsets, List<Itemset> freqItems, Itemset itemset){
		int size = itemset.size();
		
		Itemset first = new Itemset();
		first.addItem(itemset.getFirstItem());
		
		Itemset second = new Itemset();
		for(int i=1; i<size; i++) {
			second.addItem(itemset.getItemset().get(i));
		}
		
		first = findItemset(freqItems, first);
		if(second.size()==1) {
			second = findItemset(freqItems, second);
		}
		else {
			second = findItemset(freqItemsets, second);
		}
		
		rules.add(new AssociationRule(first, second, itemset.getSupport()));
		rules.add(new AssociationRule(second, first, itemset.getSupport()));
	}
}