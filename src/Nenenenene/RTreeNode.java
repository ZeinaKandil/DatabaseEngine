package Nenenenene;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

public abstract class RTreeNode implements Serializable {
	/**
	 * Abstract class that collects the common functionalities of the inner and leaf nodes
	 */
	private static final long serialVersionUID = 1L;
	protected int[] keys;
	protected int numberOfKeys;
	protected int order;
	protected int index;		//for printing the tree
	private boolean isRoot;
	private static int nextIdx = 0;
	public String TreeName;

	public RTreeNode(int order, String name) 
	{
		index = nextIdx++;
		numberOfKeys = 0;
		this.order = order;
		TreeName = name;
	}
	
	/**
	 * @return a boolean indicating whether this node is the root of the B+ tree
	 */
	public boolean isRoot()
	{
		return isRoot;
	}
	
	/**
	 * set this node to be a root or unset it if it is a root
	 * @param isRoot the setting of the node
	 */
	public void setRoot(boolean isRoot)
	{
		if (isRoot){
			deleteFile(getFilePath());
		}
		this.isRoot = isRoot;
	}
	
	/**
	 * find the key at the specified index
	 * @param index the index at which the key is located
	 * @return the key which is located at the specified index
	 */
	public int getKey(int index) 
	{
		return keys[index];
	}

	/**
	 * sets the value of the key at the specified index
	 * @param ind the index of the key to be set
	 * @param key the new value for the key
	 */
	public void setKey(int ind, int key) 
	{
		keys[ind] = key;
	}
	
	/**
	 * @return a boolean whether this node is full or not
	 */
	public boolean isFull() 
	{
		return numberOfKeys == order;
	}
	
	/**
	 * @return the last key in this node
	 */
	public int getLastKey()
	{
		return keys[numberOfKeys-1];
	}
	
	/**
	 * @return the first key in this node
	 */
	public int getFirstKey()
	{
		return keys[0];
	}
	
	/**
	 * @return the minimum number of keys this node can hold
	 */
	public abstract int minKeys();

	/**
	 * insert a key with the associated record reference in the B+ tree
	 * @param key the key to be inserted
	 * @param r a pointer to the record on the hard disk
	 * @param parent the parent of the current node
	 * @param ptr the index of the parent pointer that points to this node
	 * @param polygon 
	 * @return a key and a new node in case of a node splitting and null otherwise
	 * @throws DBAppException 
	 */
	public abstract PushUpRTree insert(int key, RefForRTree r, RTreeInnerNode parent, int ptr) throws DBAppException;
	
	public abstract ArrayList<RefForRTree> search(int key, DBPolygon polygon) throws DBAppException;
	public abstract ArrayList<RefForRTree> searchforInsert(int key, DBPolygon polygon) throws DBAppException;


	//public abstract boolean updateRef1(DBPolygon key, Ref oldRef, Ref newRef) throws DBAppException;
	/**
	 * delete a key from the B+ tree recursively
	 * @param key the key to be deleted from the B+ tree
	 * @param parent the parent of the current node
	 * @param ptr the index of the parent pointer that points to this node 
	 * @return true if this node was successfully deleted and false otherwise
	 * @throws DBAppException 
	 */
	public abstract boolean deleteEntireKey(int key, RTreeInnerNode parent, int ptr, DBPolygon polygon) throws DBAppException;
	
	public abstract boolean deleteSingleRef(int key, RTreeInnerNode parent, int ptr, RefForRTree r) throws DBAppException;
	
	public abstract boolean updateRef1(int key, RefForRTree oldRef, RefForRTree newRef) throws DBAppException;
	
	public abstract ArrayList<RefForRTree> searchMin(int key) throws DBAppException;

    public abstract ArrayList<RefForRTree> searchMax(int key) throws DBAppException;
    
    public abstract ArrayList<RefForRTree> searchLessOrEqual(int key) throws DBAppException;

    public abstract ArrayList<RefForRTree> searchBiggerOrEqual(int key) throws DBAppException;
	/**
	 * A string representation for the node
	 * @throws DBAppException 
	 */
	public String toString() {		
		String s = "(" + index + ")";

		s += "[";
		for (int i = 0; i < order; i++){
			String key = " ";
			if(i < numberOfKeys)
				key = keys[i] +"";
			
			s+= key;
			if(i < order - 1)
				s += "|";
		}
		s += "]";
		return s;
	}
	
	@SuppressWarnings("unchecked")
	public RTreeNode deserializeNode(String s) throws DBAppException {
        RTreeNode current = null;
        try {
            FileInputStream fileIn = new FileInputStream(s);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            current = (RTreeNode) in.readObject();
            in.close();
            fileIn.close();
        } catch (Exception e) {
            throw new DBAppException("No Node file with this name: " + s);
        }
        return current;
    }

    public String serializeNode(RTreeNode n) throws DBAppException {
    	String s ="";
    	try{
    		s = n.getFilePath();
    	}catch(NullPointerException np){
    		return null;
    	}
    	try {
            FileOutputStream fileOut = new FileOutputStream(s);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(n);
            out.close();
            fileOut.close();
            return s;
        } catch (IOException i) {
            throw new DBAppException("Can not serialize RNode: " + s);
        }
    }
    
    public String getFilePath(){
    	String s = "data/" + TreeName +"RTree_Node_"+ this.index + ".class";
    	return s;
    }
    
    public void setNextIdx(int n){
    	nextIdx = Math.max(nextIdx, n);
    }
    
    public boolean isEmpty(){
    	return numberOfKeys==0;
    }

    boolean deleteFile(String filename) {
        File f = new File(filename);
        return f.delete();
    }
}
