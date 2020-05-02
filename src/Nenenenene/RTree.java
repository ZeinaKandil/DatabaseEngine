package Nenenenene;

import java.awt.Polygon;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;

public class RTree implements Serializable{
	Properties config;
	private static final long serialVersionUID = 1L;
	private int order;
	public RTreeNode root;
	private String TableName;
	public String ColName;
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
	public RTree(String table, String column) throws DBAppException 
	{
        readProperty();
        String s = config.getProperty("NodeSize");
        order = Integer.parseInt(s);
        ColName = column;
		TableName = table + "_" + column;
		root = new RTreeLeafNode(order, TableName);
		root.setRoot(true);
	}
	
	/**
	 * Inserts the specified key associated with the given record in the B+ tree
	 * @param key the key to be inserted
	 * @param recordReference the reference of the record associated with the key
	 * @throws DBAppException 
	 */
	public void insert(DBPolygon polygon, Ref recordReference) throws DBAppException{
		int key = polygon.getArea(); 
		RefForRTree r = new RefForRTree(recordReference.getPage(), recordReference.getIndexInPage(), polygon);
		PushUpRTree pushUp = root.insert(key, r, null, -1);
		if(pushUp != null)
		{
			RTreeInnerNode newRoot = new RTreeInnerNode(order, TableName);
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
	public ArrayList<Ref> search(DBPolygon polygon) throws DBAppException
	{
		int key = polygon.getArea();
		ArrayList<RefForRTree> res = root.search(key, polygon);
    	Collections.sort(res);
    	ArrayList<Ref> refs = new ArrayList<Ref>();
    	for (int i = 0; i < res.size(); i++) {
			refs.add(new Ref(res.get(i).pageName, res.get(i).indexInPage));
		}
        return refs;
	}
	
	public ArrayList<Ref> searchLessOrEqual(DBPolygon polygon) throws DBAppException
	{
		int key = polygon.getArea();
		ArrayList<RefForRTree> res = root.searchLessOrEqual(key);
    	Collections.sort(res);
    	ArrayList<Ref> refs = new ArrayList<Ref>();
    	for (int i = 0; i < res.size(); i++) {
			refs.add(new Ref(res.get(i).pageName, res.get(i).indexInPage));
		}
        return refs;
	}
	
	public ArrayList<Ref> searchBiggerOrEqual(DBPolygon polygon) throws DBAppException
	{
		int key = polygon.getArea();
		ArrayList<RefForRTree> res = root.searchBiggerOrEqual(key);
    	Collections.sort(res);
    	ArrayList<Ref> refs = new ArrayList<Ref>();
    	for (int i = 0; i < res.size(); i++) {
			refs.add(new Ref(res.get(i).pageName, res.get(i).indexInPage));
		}
        return refs;
	}
	
	public ArrayList<Ref> searchforInsert(DBPolygon polygon) throws DBAppException
	{
		int key = polygon.getArea();
		ArrayList<RefForRTree> res = root.searchforInsert(key, polygon);
		Collections.sort(res);
		ArrayList<Ref> refs = new ArrayList<Ref>();
		for (int i = 0; i < res.size(); i++) {
			refs.add(new Ref(res.get(i).pageName, res.get(i).indexInPage));
		}
		return refs;
	}
	
	public boolean updateRef2(DBPolygon p, Ref oldRef, Ref newRef) throws DBAppException{
		int key = p.getArea();
		RefForRTree newR = new RefForRTree(newRef.getPage(), newRef.getIndexInPage(), p);
		RefForRTree oldR = new RefForRTree(oldRef.getPage(), oldRef.getIndexInPage(), p);
		return root.updateRef1(key, oldR, newR);
	}
	
	/**
	 * Delete a key and its associated record from the tree.
	 * @param key the key to be deleted
	 * @return a boolean to indicate whether the key is successfully deleted or it was not in the tree
	 * @throws DBAppException 
	 */
	public boolean deleteEntireKey(DBPolygon polygon) throws DBAppException
	{
		int key = polygon.getArea();
		boolean done = root.deleteEntireKey(key, null, -1, polygon);
		//go down and find the new root in case the old root is deleted
		while(root instanceof RTreeInnerNode && !root.isRoot()){
			deleteFile(root.getFilePath());
			String st = ((RTreeInnerNode) root).getFirstChild();
			root = deserializeNode(st);
		}
		deleteFile(root.getFilePath());
		return done;
	}
	
	public boolean deleteSingleRef(DBPolygon polygon, Ref r) throws DBAppException
	{
		int key = polygon.getArea();
		RefForRTree ref = new RefForRTree(r.getPage(), r.getIndexInPage(), polygon);
		boolean done = root.deleteSingleRef(key, null, -1, ref);
		//go down and find the new root in case the old root is deleted
		while(root instanceof RTreeInnerNode && !root.isRoot()){
			deleteFile(root.getFilePath());
			String st = ((RTreeInnerNode ) root).getFirstChild();
			root = deserializeNode(st);
		}
		deleteFile(root.getFilePath());
		return done;
	}
	
	public RTreeNode  deserializeNode(String s) throws DBAppException {
        RTreeNode  current = null;
        try {
            FileInputStream fileIn = new FileInputStream(s);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            current = (RTreeNode ) in.readObject();
            in.close();
            fileIn.close();
        } catch (Exception e) {
            throw new DBAppException("No Node file with this name  "+s);
        }
        return current;
    }
	
	public ArrayList<Ref> searchMin(DBPolygon polygon) throws DBAppException {
		int key = polygon.getArea();
		ArrayList<RefForRTree> res = root.searchMin(key);
    	Collections.sort(res);
    	ArrayList<Ref> refs = new ArrayList<Ref>();
    	for (int i = 0; i < res.size(); i++) {
			refs.add(new Ref(res.get(i).pageName, res.get(i).indexInPage));
		}
        return refs;
    }

    public ArrayList<Ref> searchMax(DBPolygon polygon) throws DBAppException {
    	int key = polygon.getArea();
    	ArrayList<RefForRTree> res = root.searchMax(key);
    	Collections.sort(res);
    	ArrayList<Ref> refs = new ArrayList<Ref>();
    	for (int i = 0; i < res.size(); i++) {
			refs.add(new Ref(res.get(i).pageName, res.get(i).indexInPage));
		}
        return refs;
    }
	
	/**
	 * Returns a string representation of the B+ tree.
	 */
	public String toString1() throws DBAppException {
		// <For Testing>
		// node : (id)[k1|k2|k3|k4]{P1,P2,P3,}
		String s = "";
		Queue<RTreeNode> cur = new LinkedList<RTreeNode>(), next;
		cur.add(root);
		boolean flag = false;
		while (!cur.isEmpty()) {
			next = new LinkedList<RTreeNode>();
			while (!cur.isEmpty()) {
				if (!flag) {
					RTreeNode curNode = cur.remove();
					System.out.print(curNode.toString());
					if (curNode instanceof RTreeLeafNode) {
						System.out.print("->");
					} else {
						System.out.print("{");
						RTreeInnerNode parent = (RTreeInnerNode) curNode;
						for (int i = 0; i <= parent.numberOfKeys; ++i) {
							RTreeNode x = (RTreeNode) deserializeNode(parent.getChild(i));

							System.out.print(x.index + ",");
							x.serializeNode(x);
							next.add(x);
						}
						System.out.print("} ");
						System.out.println();
					}
					cur = next;

					// </For Testing>
					flag = true;
				} else {
					RTreeNode curNode = deserializeNode(cur.remove()
							.getFilePath());
					System.out.print(curNode.toString());

					if (curNode instanceof RTreeLeafNode) {
						System.out.print("->");
						curNode.serializeNode(curNode);
					} else {
						curNode.serializeNode(curNode);
						System.out.print("{");
						RTreeInnerNode parent = (RTreeInnerNode) curNode;
						for (int i = 0; i <= parent.numberOfKeys; ++i) {
							RTreeNode x = (RTreeNode) deserializeNode(parent.getChild(i));
							System.out.print(x.index + ",");
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
		// </For Testing>
		return s;
	}
    
    public String getTableName(){
    	return TableName;
    }
    
    private static boolean deleteFile(String filename) {
        File f = new File(filename);
        return f.delete();
    }
    
    public boolean isEmpty(){
    	if(root == null) return true;
    	return root.isEmpty();
    }
    
    public static String serializePageOfRTreeRefs(String s, PageOfRTreeRefs p) throws DBAppException {
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
	
	public PageOfRTreeRefs deserializePageOfRTreeRefs(String s) throws DBAppException {
        PageOfRTreeRefs current = null;
        try {
            FileInputStream fileIn = new FileInputStream(s);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            current = (PageOfRTreeRefs) in.readObject();
            in.close();
            fileIn.close();
        } catch (Exception e) {
            throw new DBAppException("No RefForRTree file with this name: " + s);
        }
        return current;
    }
	
	public boolean updateRef1(DBPolygon p, Ref oldRef, Ref newRef){
		int key = p.getArea();
		RefForRTree newR = new RefForRTree(newRef.getPage(), newRef.getIndexInPage(), p);
		RefForRTree oldR = new RefForRTree(oldRef.getPage(), oldRef.getIndexInPage(), p);
		boolean found = false;
		int i = 0;
		while(true){
			if(found)break;
			String s = "data/" + getTableName() + "RTreeRef_" + key + "_" + i + ".class";
			i++;
			try {
				PageOfRTreeRefs page = deserializePageOfRTreeRefs(s);
				for (int j = 0; j < page.refs.size(); j++) {
					if(page.refs.get(j).equals(oldR)){
						page.refs.set(j, newR);
						found = true;
						serializePageOfRTreeRefs(s, page);
						break;
					}
				}
			} catch (DBAppException e) {
				break;
			}
			
		}
		return found;
	}

	public String printAllRef() throws DBAppException {
		String s = "";
		Queue<RTreeNode> cur = new LinkedList<RTreeNode>(), next;
		cur.add(root);
		boolean flag = false;
		while (!cur.isEmpty()) {
			next = new LinkedList<RTreeNode>();
			while (!cur.isEmpty()) {
				if (!flag) {
					RTreeNode curNode = cur.remove();
					if (curNode instanceof RTreeLeafNode) {
						for (int i = 0; i < ((RTreeLeafNode) curNode).numberOfKeys; i++) {
							if (((RTreeLeafNode) curNode).records[i] == null) {
								break;
							}
							System.out.print(((RTreeLeafNode) curNode).records[i].toString1() +" belongs to key: " + ((RTreeLeafNode) curNode).keys[i]);
						}
					} else {
						RTreeInnerNode parent = (RTreeInnerNode) curNode;
						for (int i = 0; i <= parent.numberOfKeys; ++i) {
							RTreeNode x = (RTreeNode) deserializeNode(parent.getChild(i));
							x.serializeNode(x);
							next.add(x);
						}
					}

					cur = next;
					flag = true;
				} else {
					RTreeNode curNode = deserializeNode(cur.remove().getFilePath());
					if (curNode instanceof RTreeLeafNode) {
						for (int i = 0; i < ((RTreeLeafNode) curNode).numberOfKeys
								&& curNode != null; i++) {
							if (((RTreeLeafNode) curNode).records[i] == null) {
								break;
							}
							System.out.print(((RTreeLeafNode) curNode).records[i].toString1());
						}
						curNode.serializeNode(curNode);
					} else {
						curNode.serializeNode(curNode);
						RTreeInnerNode parent = (RTreeInnerNode) curNode;
						for (int i = 0; i <= parent.numberOfKeys; ++i) {
							RTreeNode x = (RTreeNode) deserializeNode(parent
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
}