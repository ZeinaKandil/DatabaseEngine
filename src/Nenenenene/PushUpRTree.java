package Nenenenene;

public class PushUpRTree {

	/**
	 * This class is used for push keys up to the inner nodes in case
	 * of splitting at a lower level
	 */
	RTreeNode newNode;
	int key;
	
	public PushUpRTree(RTreeNode newNode, int key)
	{
		this.newNode = newNode;
		this.key = key;
	}
}
