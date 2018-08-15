package FPGrowthRuleMining;

public class AssociationRule {
	//Variables
	private Itemset left;
	private Itemset right;
	private double confidence;
	private int support;
	
	//Constructor
	public AssociationRule(Itemset left, Itemset right, int support) {
		this.left = left;
		this.right = right;
		this.confidence = ((double)support)/((double)left.getSupport());
		this.support = support;
	}
	
	//Methods
	public double getConfidence() {
		return this.confidence;
	}
	
	public void setSupport(int support) {
		this.support = support;
	}
	
	public String toString() {
		return left.toString()+" -> "+right.toString()+":"+this.support;
	}
}
