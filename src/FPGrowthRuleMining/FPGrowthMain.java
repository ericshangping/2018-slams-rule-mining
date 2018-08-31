//Frequent itemset and association rules generation using FPGrowth algorithm on hadoop
//Eric Shang-Ping Dai, DXXSHA001
//06 Aug 2018

package FPGrowthRuleMining;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
		private int offset;
		
		public void setup(Context context) {
			offset = context.getConfiguration().getInt("offset", 0);
		}
		
		//Map method
		public void map(Object key, Text line, Context context) 
		throws IOException, InterruptedException
		{
			Scanner scLine = new Scanner(line.toString());
			//Skip data
			for(int i=0; i<offset; i++) {
				scLine.next();
			}
			
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
	 * The mapper then writes the base item as a key and the path to that item as a value.
	 */
	public static class OrderedItemsetMapper
	extends Mapper<Object,Text,Text,Text>
	{
		//Variables
		private Text baseItemText = new Text();
		private Text pathText = new Text();
		private Itemset[] frequentPattern;
		private int offset;
		
		//Set frequent pattern
		public void setup(Context context) throws IOException {
			//Read frequent items found and sort by support
			offset = context.getConfiguration().getInt("offset", 0);
			List<Itemset> freqItems = ItemsetUtils.getFreqItems(context.getConfiguration(), 
					context.getConfiguration().get("hdfsOutputDir"));
			frequentPattern = new Itemset[freqItems.size()];
			freqItems.toArray(frequentPattern);
			Arrays.sort(frequentPattern);
		}
		
		public void map(Object key, Text line, Context context) 
		throws IOException, InterruptedException
		{
			Itemset orderedItemset = new Itemset();
			Itemset transaction = new Itemset();
			Scanner scLine = new Scanner(line.toString());
			//Skip data
			for(int i=0; i<offset; i++) {
				scLine.next();
			}
			
			//Construct the transaction 
			while(scLine.hasNext()) {
				transaction.addItem(scLine.next());
			}
			scLine.close();
			
			//Construct the transaction's ordered itemset
			for(Itemset set : frequentPattern) {
				if(transaction.contains(set.getLastItem()) ) {
					orderedItemset.addItem(set.getLastItem());
				}
			}
			
			//Write each path with itemset
			Itemset path = new Itemset();
			for(String item : orderedItemset.getItemset()) {
				path.addItem(item);
				baseItemText.set(item);
				pathText.set(path.toString());
				context.write(baseItemText, pathText);
			}
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
		private List<Itemset> freqItemsets;
		
		//Set up frequent items to look up counts
		public void setup(Context context) throws IOException {
			String hdfsOutputDir = context.getConfiguration().get("hdfsOutputDir");
			freqItemsets = ItemsetUtils.getFreqItems(context.getConfiguration(), hdfsOutputDir);
		}
		
		public void map(Object key, Text line, Context context) 
		throws IOException, InterruptedException
		{
			List<Itemset> itemsets = ItemsetUtils.readFreqItemsets(line.toString());
			List<AssociationRule> rules = new ArrayList<AssociationRule>();
			for(Itemset i : itemsets) {
				Itemset base = new Itemset();
				ItemsetUtils.genAssocRules(rules, itemsets, freqItemsets, base, i, i.getSupport());
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
	 * Reducer that aggregates all paths to a base item and constructs a list of all it's frequent itemsets
	 * based on its conditional pattern bases.
	 */
	public static class FreqItemsetReducer
	extends Reducer<Text,Text,Text,Text>{
		//Variables
		private Text freqItemsets = new Text();
		private Text value = new Text();
		private int support;
		private Itemset[] frequentPattern;
		
		protected void setup(Context context) throws IOException {
			this.support = context.getConfiguration().getInt("support", 0);
			List<Itemset> freqItems = ItemsetUtils.getFreqItems(context.getConfiguration(), 
					context.getConfiguration().get("hdfsOutputDir"));
			frequentPattern = new Itemset[freqItems.size()];
			freqItems.toArray(frequentPattern);
			Arrays.sort(frequentPattern);
		}
		
		public void reduce(Text baseItem, Iterable<Text> paths, Context context) 
		throws IOException, InterruptedException
		{
			//Construct conditional pattern base of the item
			List<Itemset> condPattBase = new ArrayList<Itemset>();
			int itemCount = 0;
			for(Text path : paths) {
				boolean exists = false;
				Itemset pathItemset = ItemsetUtils.readItemset(path.toString());
				pathItemset.removeLastItem();
				pathItemset.setSupport(1);
				for(Itemset i : condPattBase) {
					if(pathItemset.equals(i)) {
						i.incSupport();
						exists = true;
						break;
					}
				}
				if(!exists && pathItemset.size() >= 1) {
					condPattBase.add(pathItemset);
				}
				itemCount++;
			}
			
			if(condPattBase.size() == 0) {
				return;
			}
			
			//Construct freq itemsets
			List<Itemset> fList = ItemsetUtils.findFList(condPattBase, support);
			Itemset[] freqList = new Itemset[fList.size()];
			fList.toArray(freqList);
			Arrays.sort(freqList);
			
			List<Itemset> frequentItemsets = new ArrayList<Itemset>();
			
			//Use the f-list to find the frequent-2-itemsets
			for(Itemset itemset : freqList) {
				Itemset freqItemset = new Itemset();
				String condFreqItem = itemset.getLastItem();
				freqItemset.addItem(condFreqItem);
				freqItemset.addItem(baseItem.toString());
				freqItemset.setSupport(Math.min(itemCount, itemset.getSupport()));
				frequentItemsets.add(freqItemset);
				//Recursive call to find the rest of the itemsets
				ItemsetUtils.constructFreqItemsets(frequentItemsets, frequentPattern, condPattBase, freqItemset, support);
			}
			
			//Write the itemsets to the output
			String freqItemsets = "";
			for(Itemset freq : frequentItemsets) {
				freqItemsets += freq.toString()+":"+freq.getSupport()+";";
			}
			
			this.freqItemsets.set(freqItemsets);
			this.value.set("");
			context.write(this.freqItemsets, this.value);
		}
	}
	
	/**
	 * Reducer for rules that check rules with the minimum confidence specified.
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
			for(DoubleWritable c : values) {
				if(c.get() >= minConfidence) {
					context.write(rule, c);
				}
			}
		}
	}
	
	/**
	 * Runs the mapreduce job to find all frequent items in the dataset.
	 * @param conf Hadoop configuration variable.
	 * @param input Input directory.
	 * @param output Output directory.
	 * @return true if the job completed successfully, false if not.
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private static boolean findFrequentItems(Configuration conf, String input, String output)
	throws IOException, ClassNotFoundException, InterruptedException 
	{
		Job job = Job.getInstance(conf, "Find_Frequent_Items");
		job.setJarByClass(FPGrowthMain.class);
		job.setMapperClass(Items1Mapper.class);
		job.setReducerClass(ItemsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true);
	}
	
	/**
	 * Runs the mapreduce job that finds all frequent itemsets.
	 * @param conf Hadoop configuration variable.
	 * @param input Input directory.
	 * @param output Output directory.
	 * @return true if the job completed successfully, false if not.
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private static boolean runFrequentItemsetGeneration(Configuration conf, String input, String output) 
	throws IOException, ClassNotFoundException, InterruptedException 
	{
		Job job = Job.getInstance(conf, "Frequent_Itemsets_Generation");
		job.setJarByClass(FPGrowthMain.class);
		job.setMapperClass(OrderedItemsetMapper.class);
		job.setReducerClass(FreqItemsetReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true);
	}
	
	/**
	 * Runs the mapreduce job that generates all association rules from the frequent itemsets.
	 * @param conf Hadoop configuration variable.
	 * @param input Input directory.
	 * @param output Output directory.
	 * @return true if the job completed successfully, false if not.
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private static boolean runRuleGeneration(Configuration conf, String input, String output) 
	throws IOException, ClassNotFoundException, InterruptedException
	{
		Job job = Job.getInstance(conf, "Rule_Generation");
		job.setJarByClass(FPGrowthMain.class);
		job.setMapperClass(RulesMapper.class);
		job.setReducerClass(RulesReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true);
	}
	
	public static void main(String[] args) throws Exception {
		//Arguements
		String inputDir = args[0];
		String outputDir = args[1];
		int support = Integer.parseInt(args[2]);
		String confidence = args[3];
		int offset = Integer.parseInt(args[4]);
		
		//Program start
		long start = System.currentTimeMillis();
		
		//Hadoop program configuration settings
		boolean jobComplete;
		Configuration rulesConf = new Configuration();
		rulesConf.setInt("support", support);
		rulesConf.set("confidence", confidence);
		String hdfsOutputDir = rulesConf.get("fs.defaultFS") + outputDir;
		rulesConf.set("hdfsOutputDir", hdfsOutputDir);
		rulesConf.setInt("offset", offset);
		
		//Map-Reduce job to find frequent items
		jobComplete = findFrequentItems(rulesConf, inputDir, outputDir);
		if(!jobComplete) {
			System.out.println("An error occured while finding the frequent items.");
			System.exit(1);
		}
		
		//Generate frequent itemsets
		jobComplete = runFrequentItemsetGeneration(rulesConf, inputDir, outputDir+"FreqItemsets");
		if(!jobComplete) {
			System.out.println("An error occured while finding the frequent itemsets.");
			System.exit(1);
		}
		
		//Generate association rules
		jobComplete = runRuleGeneration(rulesConf, outputDir+"FreqItemsets", outputDir+"Rules");
		if(!jobComplete) {
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
