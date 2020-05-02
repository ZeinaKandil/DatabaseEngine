package Nenenenene;

import java.io.Serializable;
import java.util.ArrayList;

public class RTreeInnerNode extends RTreeNode  implements Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public String[] children;
	/**
	 * create RTreeNode given order.
	 * @param n
	 */
	@SuppressWarnings("unchecked")
	public RTreeInnerNode(int n, String name) 
	{
		super(n, name);
		keys = new int[n];
		children = new String[n+1];
	}

	/**
	 * get child with specified index
	 * @return Node which is child at specified index
	 */
	public String getChild(int index) 
	{
		return children[index];
	}
	
	/**
	 * creating child at specified index
	 */
	public void setChild(int index, String child) 
	{
		children[index] = child;
	}
	/**
	 * get the first child of this node.
	 * @return first child node.
	 */
	public String getFirstChild()
	{
		return children[0];
	}
	/**
	 * get the last child of this node
	 * @return last child node.
	 */
	public String getLastChild()
	{
		return children[numberOfKeys];
	}
	/**
	 * @return the minimum keys values in InnerNode
	 */
	public int minKeys()
	{
		if(this.isRoot())
			return 1;
		return (order + 2) / 2 - 1;
	}
	/**
	 * insert given key in the corresponding index.
	 * @param key key to be inserted
	 * @param Ref reference which that inserted key is located
	 * @param parent parent of that inserted node
	 * @param ptr index of pointer in the parent node pointing to the current node
	 * @return value to be pushed up to the parent.
	 * @throws DBAppException 
	 */
	public PushUpRTree insert(int key, RefForRTree recordReference, RTreeInnerNode parent, int ptr) throws DBAppException {
		int ind = findIndex(key);
		RTreeNode child = deserializeNode(children[ind]);
		PushUpRTree pushUp = child.insert(key, recordReference, this, ind);
		serializeNode(child);
		if(pushUp == null) {
			return null;
		}
		if(this.isFull()) {
			RTreeInnerNode newNode = this.split(pushUp);
			serializeNode(newNode);
			
			int newKey = newNode.getFirstKey();
			newNode.deleteAt(0, 0);
			
			return new PushUpRTree(newNode, newKey);
		}
		else
		{
			ind = 0;
			while (ind < numberOfKeys && getKey(ind) < key)
				++ind;
			this.insertRightAt(ind, pushUp.key, serializeNode(pushUp.newNode));
			return null;
		}
	}
	/**
	 * split the inner node and adjust values and pointers.
	 * @param pushup key to be pushed up to the parent in case of splitting.
	 * @return Inner node after splitting
	 * @throws DBAppException 
	 */
	@SuppressWarnings("unchecked")
	public RTreeInnerNode split(PushUpRTree pushup) throws DBAppException 
	{
		int keyIndex = this.findIndex((int)pushup.key);
		int midIndex = numberOfKeys / 2 - 1;
		if(keyIndex > midIndex)				//split nodes evenly
			++midIndex;		

		int totalKeys = numberOfKeys + 1;
		//move keys to a new node
		RTreeInnerNode newNode = new RTreeInnerNode(order, TreeName);
		for (int i = midIndex; i < totalKeys - 1; ++i) 
		{	
			newNode.insertRightAt(i - midIndex, this.getKey(i), this.getChild(i+1));
			numberOfKeys--;
		}
		newNode.setChild(0, this.getChild(midIndex));
		
		//insert the new key
		if(keyIndex < totalKeys / 2)
			this.insertRightAt(keyIndex, pushup.key, serializeNode(pushup.newNode));
		else
			newNode.insertRightAt(keyIndex - midIndex, pushup.key, serializeNode(pushup.newNode));
		

		return newNode;
	}
	
	
		/**
	 * find the correct place index of specified key in that node.
	 * @param key to be looked for
	 * @return index of that given key
	 */
	public int findIndex(int key) 
	{
		for (int i = 0; i < numberOfKeys; ++i) 
		{
			if (getKey(i) > key)
				return i;
		}
		return numberOfKeys;
	}
	/**
	 * insert at given index a given key
	 * @param index where it inserts the key
	 * @param key to be inserted at index
	 */
	private void insertAt(int index, int key) 
	{
		for (int i = numberOfKeys; i > index; --i) 
		{
			this.setKey(i, this.getKey(i - 1));
			this.setChild(i+1, this.getChild(i));
		}
		this.setKey(index, key);
		numberOfKeys++;
	}
	/**insert key and adjust left pointer with given child.
	 * @param index where key is inserted
	 * @param key to be inserted in that index
	 * @param leftChild child which this node points to with pointer at left of that index
	 * @throws DBAppException 
	 */
	public void insertLeftAt(int index, int key, String leftChild) throws DBAppException 
	{
		insertAt(index, key);
		
		this.setChild(index+1, this.getChild(index));
		this.setChild(index, leftChild);
	}
	/**insert key and adjust right pointer with given child.
	 * @param index where key is inserted
	 * @param key to be inserted in that index
	 * @param string child which this node points to with pointer at right of that index
	 * @throws DBAppException 
	 */
	public void insertRightAt(int index, int key, String rightChild) throws DBAppException
	{
		insertAt(index, key);
		this.setChild(index + 1, rightChild);
	}
	/**
	 * delete key and return true or false if it is deleted or not
	 * @throws DBAppException 
	 */
	public boolean deleteEntireKey(int key, RTreeInnerNode parent, int ptr, DBPolygon polygon) throws DBAppException 
	{
		boolean done = false;
		for(int i = 0; !done && i < numberOfKeys; ++i)
			if(keys[i] > key){
				RTreeNode B = deserializeNode(children[i]);
				done = B.deleteEntireKey(key, this, i, polygon);
				if(deleteFile(B.getFilePath())) 
					serializeNode(B);
			}
		if(!done){
			RTreeNode B = deserializeNode(children[numberOfKeys]);
			done = B.deleteEntireKey(key, this, numberOfKeys, polygon);
			if(deleteFile(B.getFilePath())) 
				serializeNode(B);
		}
		if(numberOfKeys < this.minKeys())
		{
			if(isRoot())
			{
				RTreeNode b = deserializeNode(this.getFirstChild());
				b.setRoot(true);
				serializeNode(b);
				this.setRoot(false);
				return done;
			}
			//1.try to borrow
			if(borrow(parent, ptr))
				return done;
			//2.merge
			merge(parent, ptr);
		}
		return done;
	}
	public boolean deleteSingleRef(int key, RTreeInnerNode parent, int ptr, RefForRTree r) throws DBAppException 
	{
		boolean done = false;
		for(int i = 0; !done && i < numberOfKeys; ++i)
			if(keys[i] > key){
				RTreeNode B = deserializeNode(children[i]);
				done = B.deleteSingleRef(key, this, i, r);
				if(deleteFile(B.getFilePath())) 
					serializeNode(B);
			}
		if(!done){
			RTreeNode B = deserializeNode(children[numberOfKeys]);
			done = B.deleteSingleRef(key, this, numberOfKeys, r);
			if(deleteFile(B.getFilePath())) 
				serializeNode(B);
		}
		if(numberOfKeys < this.minKeys())
		{
			if(isRoot())
			{
				RTreeNode b = deserializeNode(this.getFirstChild());
				b.setRoot(true);
				serializeNode(b);
				this.setRoot(false);
				return done;
			}
			//1.try to borrow
			if(borrow(parent, ptr))
				return done;
			//2.merge
			merge(parent, ptr);
		}
		return done;
	}
	/**
	 * borrow from the right sibling or left sibling in case of overflow.
	 * @param parent of the current node
	 * @param ptr index of pointer in the parent node pointing to the current node 
	 * @return true or false if it can borrow form right sibling or left sibling or it can not
	 * @throws DBAppException 
	 */
	public boolean borrow(RTreeInnerNode parent, int ptr) throws DBAppException
	{
		//check left sibling
		if(ptr > 0)
		{
			String childPath = parent.getChild(ptr-1);
			RTreeInnerNode leftSibling = (RTreeInnerNode) deserializeNode(childPath);
			if(leftSibling.numberOfKeys > leftSibling.minKeys())
			{
				this.insertLeftAt(0, parent.getKey(ptr-1), leftSibling.getLastChild());
				parent.deleteAt(ptr-1);
				parent.insertRightAt(ptr-1, leftSibling.getLastKey(), this.getFilePath());
				leftSibling.deleteAt(leftSibling.numberOfKeys - 1);
				serializeNode(leftSibling);
				return true;
			}
			serializeNode(leftSibling);
		}

		//check right sibling
		if(ptr < parent.numberOfKeys)
		{
			String childPath = parent.getChild(ptr+1);
			RTreeInnerNode rightSibling = (RTreeInnerNode) deserializeNode(childPath);
			if(rightSibling.numberOfKeys > rightSibling.minKeys())
			{
				this.insertRightAt(this.numberOfKeys, parent.getKey(ptr), rightSibling.getFirstChild());
				parent.deleteAt(ptr);
				parent.insertRightAt(ptr, rightSibling.getFirstKey(), rightSibling.getFilePath());
				rightSibling.deleteAt(0, 0);
				serializeNode(rightSibling);
				return true;
			}
			serializeNode(rightSibling);
		}
		return false;
	}
	/**
	 * try to merge with left or right sibling in case of overflow
	 * @param parent of the current node 
	 * @param ptr index of pointer in the parent node pointing to the current node
	 * @throws DBAppException 
	 */
	public void merge(RTreeInnerNode parent, int ptr) throws DBAppException
	{
		if(ptr > 0)
		{
			//merge with left
			String childPath = parent.getChild(ptr-1);
			RTreeInnerNode leftSibling = (RTreeInnerNode) deserializeNode(childPath);
			leftSibling.merge(parent.getKey(ptr-1), this);	
			serializeNode(leftSibling);
			deleteFile(this.getFilePath());
			parent.deleteAt(ptr-1);		
		}
		else
		{
			//merge with right
			String childPath = parent.getChild(ptr+1);
			RTreeInnerNode rightSibling = (RTreeInnerNode) deserializeNode(childPath);
			this.merge(parent.getKey(ptr), rightSibling);
			serializeNode(rightSibling);
			deleteFile(rightSibling.getFilePath());
			parent.deleteAt(ptr);
		}
	}
	
	/**
	 * merge the current node with the passed node and pulling the passed key from the parent
	 * to be inserted with the merged node
	 * @param parentKey the pulled key from the parent to be inserted in the merged node
	 * @param foreignNode the node to be merged with the current node
	 * @throws DBAppException 
	 */
	public void merge(int parentKey, RTreeInnerNode foreignNode) throws DBAppException
	{
		this.insertRightAt(numberOfKeys, parentKey, foreignNode.getFirstChild());
		for(int i = 0; i < foreignNode.numberOfKeys; ++i)
			this.insertRightAt(numberOfKeys, foreignNode.getKey(i), foreignNode.getChild(i+1));
	}

	/**
	 * delete the key at the specified index with the option to delete the right or left pointer
	 * @param keyIndex the index whose key will be deleted
	 * @param childPtr 0 for deleting the left pointer and 1 for deleting the right pointer
	 */
	public void deleteAt(int keyIndex, int childPtr)	//0 for left and 1 for right
	{

		for(int i = keyIndex; i < numberOfKeys - 1; ++i)
		{
			keys[i] = keys[i+1];
			children[i+childPtr] = children[i+childPtr+1];
		}
		if(childPtr == 0)
			children[numberOfKeys-1] = children[numberOfKeys];
		numberOfKeys--;
	}
	
	/**
	 * searches for the record reference of the specified key
	 * @throws DBAppException 
	 */
	@Override
	public ArrayList<RefForRTree> search(int key, DBPolygon polygon) throws DBAppException 
	{
		RTreeNode B = deserializeNode((children[findIndex(key)]));
		ArrayList<RefForRTree> r = B.search(key, polygon);
		return r;
	}
	public ArrayList<RefForRTree> searchforInsert(int key, DBPolygon polygon) throws DBAppException
	{
		RTreeNode B = deserializeNode((children[findIndex(key)]));
		ArrayList<RefForRTree> r = B.searchforInsert(key, polygon);
		return r;
	}
	
	public boolean updateRef1(int key, RefForRTree oldRef, RefForRTree newRef) throws DBAppException{
		boolean done=false;
//		Should this be changed to numberOfKeys instead of children.length?
		for(int i=0;i<children.length;i++) {
			if(children[i] == null)
		    	break;
			RTreeNode B = deserializeNode((children[i]));
			done = B.updateRef1(key, oldRef, newRef);
			serializeNode(B);
		}
		return done;
	}
	
	/**
	 * delete the key at the given index and deleting its right child
	 */
	public void deleteAt(int index) 
	{
		deleteAt(index, 1);	
	}
	
	public ArrayList<RefForRTree> searchMin(int key) throws DBAppException {
        RTreeNode B = deserializeNode(getFirstChild());
        ArrayList<RefForRTree> r = B.searchMin(key);
        return r;
    }
	
	public ArrayList<RefForRTree> searchLessOrEqual(int key) throws DBAppException {
        RTreeNode B = deserializeNode(getFirstChild());
        ArrayList<RefForRTree> r = B.searchLessOrEqual(key);
        return r;
    }

    public ArrayList<RefForRTree> searchMax(int key) throws DBAppException {
        RTreeNode B = deserializeNode((getLastChild()));
        ArrayList<RefForRTree> r = B.searchMax(key);
        return r;
    }
    
    public ArrayList<RefForRTree> searchBiggerOrEqual(int key) throws DBAppException {
        RTreeNode B = deserializeNode((getLastChild()));
        ArrayList<RefForRTree> r = B.searchBiggerOrEqual(key);
        return r;
    }
}
