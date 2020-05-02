package Nenenenene;

import java.io.Serializable;
import java.util.ArrayList;

public class PageOfRTreeRefs implements Serializable {
	ArrayList<RefForRTree> refs;
	int n;
	String Treename, key;
	public int index;		
	
	
	public PageOfRTreeRefs(String Tree, String Key, int idx, int N) throws DBAppException{
		n = N;
		Treename = Tree;
		key = Key;
		refs = new ArrayList<RefForRTree>();
		index = idx;
	}

    
    public String getFilePath(){
    	String s = "data/" + Treename + "RTreeRef_" + key + "_" + index + ".class";
    	return s;
    }
	
    public String toString(){
    	return "\n" + "Ref Page with key "+key+" " + "\n" + refs.toString();
    }
			
	
}
