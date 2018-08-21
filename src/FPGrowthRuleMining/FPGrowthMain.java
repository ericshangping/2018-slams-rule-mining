//Frequent itemset and association rules generation using FPGrowth algorithm
//DXXSHA001
//06 Aug 2018

package FPGrowthRuleMining;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Scanner;

public class FPGrowthMain {
	
	/**
	 * Mapper to find candidate-1-itemsets for the IBM dataset
	 * Processes each line at a time.
	 */
	public static class Items1Mapper
	extends Mapper<Object,Text,Text,IntWritable>
	{
		//Variables
		private Text frequent1Set = new Text();
		private final IntWritable one = new IntWritable(1);
		
		//Map method
		public void map(Object key, Text line, Context context) 
		throws IOException, InterruptedException
		{
			Scanner scLine = new Scanner(line.toString());
			//Skip transaction ID and customer ID
			scLine.next();
			scLine.next();
			
			//Candidate-1-itemsets
			while(scLine.hasNext()) {
				String item = scLine.next();
				Itemset candidateItemset = new Itemset();
				candidateItemset.addItem(item);
				frequent1Set.set(candidateItemset.toString());
				context.write(frequent1Set, one);
			}
			scLine.close();
		}
	}
	
	/**
	 * Mapper that maps each transaction to an ordered itemset.
	 * Order defined by the frequent pattern.
	 */
	public static class OrderedItemsetMapper
	extends Mapper<Object,Text,Text,IntWritable>
	{
		//Variables
		private Text orderedItemsetText = new Text();
		private final IntWritable one = new IntWritable(1);
		private String[] frequentPattern;
		
		//Set frequent pattern
		public void setup(Context context) {
			frequentPattern = context.getConfiguration().get("frequentPattern").split(",");
		}
		
		public void map(Object key, Text line, Context context) 
		throws IOException, InterruptedException
		{
			Itemset orderedItemset = new Itemset();
			Itemset transaction = new Itemset();
			Scanner scLine = new Scanner(line.toString());
			//Skip transaction ID and customer ID
			scLine.next();
			scLine.next();
			
			//Construct the transaction 
			while(scLine.hasNext()) {
				transaction.addItem(scLine.next());
			}
			scLine.close();
			
			//Construct the transaction's ordered itemset
			for(String item : frequentPattern) {
				if(transaction.contains(item) ) {
					orderedItemset.addItem(item);
				}
			}
			
			orderedItemsetText.set(orderedItemset.toString());
			context.write(orderedItemsetText, one);
		}
	}
	
	/**
	 * Mapper that maps a line of frequent itemsets with combinations of rules.
	 */
	public static class RulesMapper
	extends Mapper<Object,Text,Text,DoubleWritable>
	{
		private Text assocRule = new Text();
		private DoubleWritable confidence = new DoubleWritable();
		private List<Itemset> freqItemsets = new ArrayList<Itemset>();
		
		//Set up frequent items to look up counts
		public void setup(Context context) throws IOException {
			String hdfsOutputDir = context.getConfiguration().get("fs.defaultFS") + 
					context.getConfiguration().get("outputDir");
			List<Itemset> freq1Itemsets = ItemsetUtils.getItem1Sets(context.getConfiguration(), hdfsOutputDir);
			for(Itemset i : freq1Itemsets) {
				freqItemsets.add(i);
			}
		}
		
		public void map(Object key, Text line, Context context) 
		throws IOException, InterruptedException
		{
			/*if(line.toString().equals("")||line.toString().equals("\n")) {
				return;
			}*/
			
			List<Itemset> itemsets = ItemsetUtils.readFreqItemsets(line.toString());
			List<AssociationRule> rules = new ArrayList<AssociationRule>();
			for(Itemset i : itemsets) {
				ItemsetUtils.genAssocRules(rules, itemsets, freqItemsets, i);
			}
			
			for(AssociationRule r : rules) {
				assocRule.set(r.toString());
				confidence.set(r.getConfidence());
				context.write(assocRule, confidence);
			}
		}
	}
	
	/**
	 * Reducer to aggregate Text keys and accumulate count
	 */
	public static class ItemsReducer
	extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		//Variables
		private Text freqItemset = new Text();
		private int support;
		
		//Setup - set support 
		protected void setup(Context context) {
			this.support = context.getConfiguration().getInt("support", 0);
		}
		
		//Reduce method
		public void reduce(Text items, Iterable<IntWritable> values, Context context) 
		throws IOException, InterruptedException 
		{
			int sum = 0;
		    for (IntWritable i : values) {
		    		sum += i.get();
			}
		    
		    if(sum >= support) {
		    		IntWritable count = new IntWritable(sum);
		    		Itemset itemset = ItemsetUtils.readItemset(items.toString());
		    		freqItemset.set(itemset.toString());
		    		context.write(freqItemset, count);
		    }
		}
	}
	
	/**
	 * Reducer for rules that check rules with the minimum confidence specified
	 */
	public static class RulesReducer
	extends Reducer<Text,DoubleWritable,Text,DoubleWritable>
	{
		private double minConfidence;
		
		protected void setup(Context context) {
			String confString = context.getConfiguration().get("confidence", "0");
			minConfidence = Double.parseDouble(confString);
		}
		
		public void reduce(Text rule, Iterable<DoubleWritable> values, Context context) 
		throws IOException, InterruptedException
		{
			//should only do once
			for(DoubleWritable c : values) {
				if(c.get() >= minConfidence) {
					context.write(rule, c);
				}
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		//Arguements
		String inputDir = args[0];
		String outputDir = args[1];
		int support = Integer.parseInt(args[2]);
		String confidence = args[3];
		
		//Program start
		long start = System.currentTimeMillis();
		
		//Hadoop program configuration settings
		Configuration rulesConf = new Configuration();
		rulesConf.setInt("support", support);
		rulesConf.set("confidence", confidence);
		rulesConf.set("outputDir", outputDir);
		FileSystem fs = FileSystem.get(rulesConf);
		String hdfsOutputDir = rulesConf.get("fs.defaultFS") + outputDir;
		
		//Map-Reduce job to find frequent items
		Job freq1Sets = Job.getInstance(rulesConf, "FPGrowth_Frequent-1-Itemsets");
		freq1Sets.setJarByClass(FPGrowthMain.class);
		freq1Sets.setMapperClass(Items1Mapper.class);
		freq1Sets.setReducerClass(ItemsReducer.class);
		freq1Sets.setOutputKeyClass(Text.class);
		freq1Sets.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(freq1Sets, new Path(inputDir));
		FileOutputFormat.setOutputPath(freq1Sets, new Path(outputDir));
		
		boolean freq1SetComplete = freq1Sets.waitForCompletion(true);
		if(!freq1SetComplete) {
			System.out.println("An error occured while finding the frequent items.");
			System.exit(1);
		}
		
		//Construct frequent pattern by sorting frequent items by count
		List<Itemset> freqItems = ItemsetUtils.getItem1Sets(rulesConf, hdfsOutputDir);
		Itemset[] frequentPattern = new Itemset[freqItems.size()];
		freqItems.toArray(frequentPattern);
		Arrays.sort(frequentPattern);
		Hashtable<String, Integer> freqPatternTable = new Hashtable<String, Integer>();
		
		String frequentPatternString = "";
		for(int i=0; i<frequentPattern.length; i++) {
			String item = frequentPattern[i].toString();
			item = item.replaceAll("\\[", "");
			item = item.replaceAll("\\]", "");
			freqPatternTable.put(item, i);
			frequentPatternString += item.toString()+",";
		}
		frequentPatternString = frequentPatternString.substring(0, frequentPatternString.length()-1);
		
		//Job to find ordered itemsets and find cumulative frequencies 
		//Based on ordering of the frequent pattern
		rulesConf.set("frequentPattern", frequentPatternString);
		rulesConf.setInt("support", 0);
		Job orderedItemsets = Job.getInstance(rulesConf, "FPGrowth_Ordered_Itemsets");
		orderedItemsets.setJarByClass(FPGrowthMain.class);
		orderedItemsets.setMapperClass(OrderedItemsetMapper.class);
		orderedItemsets.setReducerClass(ItemsReducer.class);
		orderedItemsets.setOutputKeyClass(Text.class);
		orderedItemsets.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(orderedItemsets, new Path(inputDir));
		FileOutputFormat.setOutputPath(orderedItemsets, new Path(outputDir+"OrderedItemsets"));
		
		boolean orderedItemsetsComplete = orderedItemsets.waitForCompletion(true);
		if(!orderedItemsetsComplete) {
			System.out.println("An error occured while finding the ordered itemsets.");
			System.exit(1);
		}
		
		//Construct FP Tree with ordered itemsets
		FPTreeNode fpTreeRoot = new FPTreeNode();
		FPTreeNode current = fpTreeRoot;
		FPTreeNode[] nodeLinkTable = new FPTreeNode[frequentPattern.length];
		
		//Read ordered itemset output parts
		String orderedOutputFileBase = hdfsOutputDir + "OrderedItemsets" + "/part-r-";
		int part = 0;
		String partString = ""+part;
		String orderedOutputFile = orderedOutputFileBase + (("00000"+partString).substring(partString.length()));
		Path path = new Path(orderedOutputFile);
		
		do {
			//Read in ordered itemsets and construct the FP tree
			Scanner scOutput = new Scanner(fs.open(path));
			while(scOutput.hasNextLine()) {
				String[] keyValue = scOutput.nextLine().split("\t");
				if(keyValue[0].equals("[]")) {
					continue;
				}
				Itemset orderedItemset = ItemsetUtils.readItemset(keyValue[0]);
				int count = Integer.parseInt(keyValue[1]);
				
				//For all the items in the ordered itemset of the ordered transaction
				current = fpTreeRoot;
				for(String item : orderedItemset.getItemset()) {
					//Add to the cumulative counts of the associations
					boolean childExisted = current.hasChild(item) != (-1);
					FPTreeNode child = current.addChild(item, count);
					
					//Add the node to node link table
					int tableIndex = freqPatternTable.get(item);
					
					//Adding the first node
					if(nodeLinkTable[tableIndex] == null) {
						nodeLinkTable[tableIndex] = child;
					}
					//Appending it to the end of the next-chain, only if it is a new node
					else if(!childExisted) {
						FPTreeNode next = nodeLinkTable[tableIndex];
						while(next.hasNext()) {
							next = next.getNext();
						}
						next.setNext(child);
					}
					//End - adding to node link table
					current = child;
				}
			}
			scOutput.close();
			
			//Check for the next file
			part++;
			partString = ""+part;
			orderedOutputFile = orderedOutputFileBase + (("00000"+partString).substring(partString.length()));
			path = new Path(orderedOutputFile);
		}while(fs.exists(path));
		
		//Generate frequent itemsets from FP tree and write them to a file for rule generating
		String freqItemsetsFile = hdfsOutputDir + "FreqItemsets/freqItemsets.data";
		Path outFile = new Path(freqItemsetsFile);
		if(fs.exists(outFile)) {
			System.out.println("The output file for frequent itemsets already exists.");
			System.out.println("Location: "+freqItemsetsFile);
		}
		FSDataOutputStream outStream = fs.create(outFile);
		
		//Construct frequent itemsets for each node in the node link table
		for(int i=0; i<nodeLinkTable.length; i++) {
			//List of the original conditional pattern bases of each node in the node link table
			List<Itemset> condPattBaseList = new ArrayList<Itemset>();
			int itemCount = 0;
			
			if(nodeLinkTable[i] == null) {
				continue;
			}
			
			//Construct conditional pattern bases
			FPTreeNode nextClimber = nodeLinkTable[i];
			do {
				itemCount += nextClimber.getCount();
				FPTreeNode parentClimber = nextClimber.getParent();
				Itemset condPatternBase = new Itemset();
				//Path from  the node to the root
				while(parentClimber.hasParent()) {
					condPatternBase.addItem(parentClimber.getItem());
					parentClimber = parentClimber.getParent();
				}
				condPatternBase.reverseSet();
				condPatternBase.setSupport(nextClimber.getCount());
				if(condPatternBase.size() >= 1) {
					condPattBaseList.add(condPatternBase);
				}
				//Loops for each node in the linked list
				nextClimber = nextClimber.getNext();
			}while(nextClimber != null);

			//If there is no conditional pattern bases for the item
			if(condPattBaseList.size() == 0) {
				continue;
			}
			
			//Mine frequent itemsets by constructing the conditional FP tree of each frequent item
			List<Itemset> fList = ItemsetUtils.findFList(condPattBaseList, support);
			Itemset[] freqList = new Itemset[fList.size()];
			fList.toArray(freqList);
			Arrays.sort(freqList);
			
			List<Itemset> frequentItemsets = new ArrayList<Itemset>();
			//Use the f-list to find the frequent-2-itemsets
			for(Itemset itemset : freqList) {
				Itemset freqItemset = new Itemset();
				String condFreqItem = itemset.getLastItem();
				freqItemset.addItem(condFreqItem);
				freqItemset.addItem(nodeLinkTable[i].getItem());
				freqItemset.setSupport(Math.min(itemCount, itemset.getSupport()));
				frequentItemsets.add(freqItemset);
				//Recursive call to find the rest of the itemsets
				ItemsetUtils.constructFreqItemsets(frequentItemsets, frequentPattern, condPattBaseList, freqItemset, support);
			}
			
			//Write the itemsets to the file
			for(Itemset freq : frequentItemsets) {
				outStream.writeBytes(freq.toString()+":"+freq.getSupport()+";");
			}
			if(nodeLinkTable[i]!=null && frequentItemsets.size()!=0) {
				outStream.writeBytes("\n");
			}
		}
		outStream.close();
		
		//Generate association rules
		Job rules = Job.getInstance(rulesConf, "Rule_Generation");
		rules.setJarByClass(FPGrowthMain.class);
		rules.setMapperClass(RulesMapper.class);
		rules.setReducerClass(RulesReducer.class);
		rules.setOutputKeyClass(Text.class);
		rules.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(rules, new Path(outputDir+"FreqItemsets"));
		FileOutputFormat.setOutputPath(rules, new Path(outputDir+"Rules"));
		
		boolean rulesComplete = rules.waitForCompletion(true);
		if(!rulesComplete) {
			System.out.println("An error occured while generating rules.");
			System.exit(1);
		}
		
		//Print computation time
		long end = System.currentTimeMillis();
		long time = (end-start)/1000;
		System.out.println("Rules generated. Time: "+time+"s");
		
		System.exit(0);
	}
}
