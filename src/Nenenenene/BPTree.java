package Nenenenene;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;

public class BPTree<T extends Comparable<T>> implements Serializable{
	Properties config;
	private static final long serialVersionUID = 1L;
	private int order;
	public BPTreeNode<T> root;
	private String TableName;
	public String ColName;
	public String table;
	public String typeOfKey;
	/**
     * The branching factor or order for the B+ tree, that measures the capacity of nodes
     * (i.e., the number of children nodes) for internal nodes in the tree.
     */

    public void readProperty() throws DBAppException {
        config = new Properties();
        try {
            FileInputStream f = new FileInputStream("config/DBApp.properties");
            config.load(f);
        } catch (IOException e) {
            throw new DBAppException("Problem with reading the config file.");
        }

    }
    /* config file has the branching factor*/
    
    /**
	 * Creates an empty B+ tree
	 * @param order the maximum number of keys in the nodes of the tree
     * @throws DBAppException 
	 */
	public BPTree(String table, String column) throws DBAppException 
	{
        readProperty();
        String s = config.getProperty("NodeSize");
        order = Integer.parseInt(s);
        ColName = column;
        this.table = table;
        typeOfKey = getType(table, column);
		TableName = table + "_" + column;
		root = new BPTreeLeafNode<T>(order, TableName);
		root.setRoot(true);
	}
	
	/**
	 * Inserts the specified key associated with the given record in the B+ tree
	 * @param key the key to be inserted
	 * @param recordReference the reference of the record associated with the key
	 * @throws DBAppException 
	 */
	public void insert(T key, Ref recordReference) throws DBAppException{
		PushUp<T> pushUp = root.insert(key, recordReference, null, -1);
		if(pushUp != null)
		{
			BPTreeInnerNode<T> newRoot = new BPTreeInnerNode<T>(order, TableName);
			newRoot.insertLeftAt(0, pushUp.key, root.getFilePath());
			newRoot.setChild(1, pushUp.newNode.getFilePath());
			root.serializeNode(pushUp.newNode);
			root.setRoot(false);
			root.serializeNode(root);
			root = newRoot;
			deleteFile(root.getFilePath());
			root.setRoot(true);
		}
	}
	
	
	/**
	 * Looks up for the record that is associated with the specified key
	 * @param key the key to find its record
	 * @return the reference of the record associated with this key 
	 * @throws DBAppException 
	 */
	public ArrayList<Ref> search(T key) throws DBAppException
	{
		return root.search(key);
	}
	public ArrayList<Ref> searchforInsert(T key) throws DBAppException
	{
		return root.searchforInsert(key);
	}
	
	public boolean updateRef2(T key, Ref oldRef, Ref newRef) throws DBAppException{
		return root.updateRef1(key, oldRef, newRef);
	}
	
	
	/**
	 * Delete a key and its associated record from the tree.
	 * @param key the key to be deleted
	 * @return a boolean to indicate whether the key is successfully deleted or it was not in the tree
	 * @throws DBAppException 
	 */
	public boolean deleteEntireKey(T key) throws DBAppException
	{
		boolean done = root.deleteEntireKey(key, null, -1);
		//go down and find the new root in case the old root is deleted
		while(root instanceof BPTreeInnerNode && !root.isRoot()){
			deleteFile(root.getFilePath());
			String st = ((BPTreeInnerNode<T>) root).getFirstChild();
			root = deserializeNode(st);
		}
		deleteFile(root.getFilePath());
		return done;
	}
	
	public boolean deleteSingleRef(T key, Ref r) throws DBAppException
	{
		boolean done = root.deleteSingleRef(key, null, -1, r);
		//go down and find the new root in case the old root is deleted
		while(root instanceof BPTreeInnerNode && !root.isRoot()){
			deleteFile(root.getFilePath());
			String st = ((BPTreeInnerNode<T>) root).getFirstChild();
			root = deserializeNode(st);
		}
		deleteFile(root.getFilePath());
		return done;
	}
	
	public BPTreeNode<T> deserializeNode(String s) throws DBAppException {
        BPTreeNode<T> current = null;
        try {
            FileInputStream fileIn = new FileInputStream(s);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            current = (BPTreeNode<T>) in.readObject();
            in.close();
            fileIn.close();
        } catch (Exception e) {
            throw new DBAppException("No Node file with this name  "+s);
        }
        return current;
    }
	
	public ArrayList<Ref> searchMin(T key) throws DBAppException {
        return root.searchMin(key);
    }

    public ArrayList<Ref> searchMax(T key) throws DBAppException {
        return root.searchMax(key);
    }
	
	/**
	 * Returns a string representation of the B+ tree.
	 */
    public String toString1() throws DBAppException {	
		//	<For Testing>
		// node :  (id)[k1|k2|k3|k4]{P1,P2,P3,}
		String s = "";
		Queue<BPTreeNode<T>> cur = new LinkedList<BPTreeNode<T>>(), next;
		cur.add(root);
		boolean flag=false;
		while(!cur.isEmpty())
		{
			next = new LinkedList<BPTreeNode<T>>();
			while(!cur.isEmpty())
			{
				if(!flag) {
					BPTreeNode<T> curNode = cur.remove();
					System.out.print(curNode.toString());
			
					if(curNode instanceof BPTreeLeafNode) {
						System.out.print("->");
					}
					else
					{
						System.out.print("{");
						BPTreeInnerNode<T> parent = (BPTreeInnerNode<T>) curNode;
						for(int i = 0; i <= parent.numberOfKeys; ++i)
						{
							BPTreeNode<T> x = (BPTreeNode<T>) deserializeNode(parent.getChild(i));
							
							System.out.print(x.index+",");
							x.serializeNode(x);
							next.add(x);
						}
						System.out.print("} ");
						System.out.println();
					}
				cur = next;
				
			//	</For Testing>
				flag=true;	
				}
				else {
				BPTreeNode<T> curNode = deserializeNode(cur.remove().getFilePath());
				System.out.print(curNode.toString());
			       
				if(curNode instanceof BPTreeLeafNode) {
					System.out.print("->");
				    curNode.serializeNode(curNode);
				}
			
				else
				{
					curNode.serializeNode(curNode);
					System.out.print("{");
					BPTreeInnerNode<T> parent = (BPTreeInnerNode<T>) curNode;
					for(int i = 0; i <= parent.numberOfKeys; ++i)
					{
						BPTreeNode<T> x = (BPTreeNode<T>) deserializeNode(parent.getChild(i));
						
						System.out.print(x.index+",");
						x.serializeNode(x);
						next.add(x);
					}
					System.out.print("} ");
					System.out.println();
			}
			cur = next;
		}
		}
		}
		//	</For Testing>
		return s;
	}
    
    private static boolean deleteFile(String filename) {
        File f = new File(filename);
        return f.delete();
    }
    
    public String getTableName() {
		return TableName;
	}
    
    public boolean isEmpty(){
    	if(root == null) return true;
    	return root.isEmpty();
    }
    
	public String printAllRef() throws DBAppException {
		String s = "";
		Queue<BPTreeNode<T>> cur = new LinkedList<BPTreeNode<T>>(), next;
		cur.add(root);
		boolean flag = false;
		while (!cur.isEmpty()) {
			next = new LinkedList<BPTreeNode<T>>();
			while (!cur.isEmpty()) {
				if (!flag) {
					BPTreeNode<T> curNode = cur.remove();
					if (curNode instanceof BPTreeLeafNode) {
						for (int i = 0; i < ((BPTreeLeafNode) curNode).numberOfKeys; i++) {
							if (((BPTreeLeafNode) curNode).records[i] == null) {
								break;
							}
							System.out.print(((BPTreeLeafNode) curNode).records[i].toString1() +" belongs to key: " + ((BPTreeLeafNode) curNode).keys[i]);
						}
					} else {
						BPTreeInnerNode<T> parent = (BPTreeInnerNode<T>) curNode;
						for (int i = 0; i <= parent.numberOfKeys; ++i) {
							BPTreeNode<T> x = (BPTreeNode<T>) deserializeNode(parent.getChild(i));
							x.serializeNode(x);
							next.add(x);
						}
					}

					cur = next;
					flag = true;
				} else {
					BPTreeNode<T> curNode = deserializeNode(cur.remove().getFilePath());
					if (curNode instanceof BPTreeLeafNode) {
						for (int i = 0; i < ((BPTreeLeafNode) curNode).numberOfKeys
								&& curNode != null; i++) {
							if (((BPTreeLeafNode) curNode).records[i] == null) {
								break;
							}
							System.out.print(((BPTreeLeafNode) curNode).records[i].toString1());
						}
						curNode.serializeNode(curNode);
					} else {
						curNode.serializeNode(curNode);
						BPTreeInnerNode<T> parent = (BPTreeInnerNode<T>) curNode;
						for (int i = 0; i <= parent.numberOfKeys; ++i) {
							BPTreeNode<T> x = (BPTreeNode<T>) deserializeNode(parent
									.getChild(i));
							x.serializeNode(x);
							next.add(x);
						}
					}
					cur = next;
				}
			}
		}
		return s;
	}
    
    
	public String printAllkeys() throws DBAppException {
		String s = "";
		Queue<BPTreeNode<T>> cur = new LinkedList<BPTreeNode<T>>(), next;
		cur.add(root);
		boolean flag = false;
		while (!cur.isEmpty()) {
			next = new LinkedList<BPTreeNode<T>>();
			while (!cur.isEmpty()) {
				if (!flag) {
					BPTreeNode<T> curNode = cur.remove();
					if (curNode instanceof BPTreeLeafNode) {
						for (int i = 0; i < ((BPTreeLeafNode) curNode).records.length; i++) {
							if (((BPTreeLeafNode) curNode).records[i] == null) {
								break;
							}
							((BPTreeLeafNode) curNode).records[i].printKeys();
						}
						System.out.println();
					} else {
						BPTreeInnerNode<T> parent = (BPTreeInnerNode<T>) curNode;
						for (int i = 0; i <= parent.numberOfKeys; ++i) {
							BPTreeNode<T> x = (BPTreeNode<T>) deserializeNode(parent
									.getChild(i));
							x.serializeNode(x);
							next.add(x);
						}
					}
					cur = next;
					flag = true;

				} else {
					BPTreeNode<T> curNode = deserializeNode(cur.remove()
							.getFilePath());
					if (curNode instanceof BPTreeLeafNode) {
						for (int i = 0; i < ((BPTreeLeafNode) curNode).records.length
								&& curNode != null; i++) {
							if (((BPTreeLeafNode) curNode).records[i] == null) {
								break;
							}
							((BPTreeLeafNode) curNode).records[i].printKeys();
						}
						curNode.serializeNode(curNode);
						System.out.println();
					}

					else {
						curNode.serializeNode(curNode);
						BPTreeInnerNode<T> parent = (BPTreeInnerNode<T>) curNode;
						for (int i = 0; i <= parent.numberOfKeys; ++i) {
							BPTreeNode<T> x = (BPTreeNode<T>) deserializeNode(parent
									.getChild(i));

							x.serializeNode(x);
							next.add(x);
						}
					}
					cur = next;
				}
			}
		}
		return s;
	}
    
    
    public static String serializePageOfRef(String s, PageOfRef p) throws DBAppException {
        FileOutputStream fileOut;
		try {
			fileOut = new FileOutputStream(s);
		} catch (FileNotFoundException e) {
			throw new DBAppException("FileNotFound when serializing: " + s);
		}
        ObjectOutputStream out;
		try {
			out = new ObjectOutputStream(fileOut);
		} catch (IOException e) {
			throw new DBAppException("IOException when serializing: " + s);
		}
		try {
			out.writeObject(p);
		} catch (IOException e) {
			throw new DBAppException("IOException when writingObject in serialization of " + s);
		}
		try {
			out.close();
			fileOut.close();
		} catch (IOException e) {
			throw new DBAppException("IOException when serializing " + s + " in the closing bit");
		}
        return s;
	}
	
	public PageOfRef deserializePageOfRef(String s) throws DBAppException {
        PageOfRef current = null;
        try {
            FileInputStream fileIn = new FileInputStream(s);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            current = (PageOfRef) in.readObject();
            in.close();
            fileIn.close();
        } catch (Exception e) {
            throw new DBAppException("No Ref file with this name: " + s);
        }
        return current;
    }
	
	public boolean updateRef1(T key, Ref oldRef, Ref newRef) throws DBAppException{
		boolean found = false;
		int i = 0;
		while(true){
			if(found)break;
			String s = "data/" + getTableName() + "Ref" + key.toString().trim().replace(' ', '_').replace(':', '_') + "_" + i + ".class";
			i++;
			try {
				PageOfRef page = deserializePageOfRef(s);
				for (int j = 0; j < page.refs.size(); j++) {
					if(page.refs.get(j).equals(oldRef)){
						page.refs.set(j, newRef);
						serializePageOfRef(s, page);
						found = true;
						break;
					}
				}
			} catch (DBAppException e) {
				break;
			}
			
		}
		return found;
	}
	
	public String getType(String table, String col) throws DBAppException{
		BufferedReader br;
        String s = "";
        try {
            br = new BufferedReader(new FileReader("data/metadata.csv"));
            s = br.readLine();
        } catch (FileNotFoundException e) {
            throw new DBAppException("can not find metadata file");
        } catch (IOException IO) {
            throw new DBAppException("can not write to metadata file");
        }
        try {
            while (br.ready()) {
                s = br.readLine();
                String[] st = s.split(", ");
                if (!st[0].equals(table) || !st[1].equals(col)) continue;
                String value = st[2];
               return value;
            }
        } catch (Exception e) {
            throw new DBAppException("Can't write to metadata file or type entered is iincorrecyt");
        }
        return s;
	}
}