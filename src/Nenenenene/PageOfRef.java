package Nenenenene;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Properties;

public class PageOfRef implements Serializable {
	ArrayList<Ref> refs;
	int n;
	public String Treename, key;
	public int index;		//for printing the tree
//	public static int nextIdx = 0;
	
	
	public PageOfRef(String Tree, String Key, int idx, int N) throws DBAppException{
		n = N;
		Treename = Tree;
		key = Key;
		refs = new ArrayList<Ref>();
		index = idx;
	}

    
    public String getFilePath(){
    	
    	String k = key.trim().replace(' ', '_');
    	String s = "data/" + Treename + "Ref" + k.replace(':', '_') + "_" + index + ".class";
    	return s;
    }
    
    public String toString(){
    	return "Ref Page with key "+key+" " + "\n" + refs.toString();
    }
	
	
			
	
}
