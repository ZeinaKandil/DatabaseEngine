package Nenenenene;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

public class GroupsOfRef implements Serializable{
	public ArrayList<String> pagesOfRef;
	int N;
	
	public GroupsOfRef(int n){
		N = n;
		pagesOfRef = new ArrayList<String>();
	}
	
	public void addRef(Ref r, String Tree, String Key) throws DBAppException{
		if(pagesOfRef.size()==0){
			PageOfRef p = new PageOfRef(Tree, Key,0, N);
			String filePath = p.getFilePath();
			pagesOfRef.add(filePath);
			p.refs.add(r);
			serializePageOfRef(filePath, p);
			return;
		}
		int last = pagesOfRef.size()-1;
		PageOfRef lastPage = deserializePageOfRef(pagesOfRef.get(last));
		if(lastPage.refs.size() < lastPage.n){
			lastPage.refs.add(r);
			serializePageOfRef(pagesOfRef.get(last), lastPage);
		}else{
			serializePageOfRef(pagesOfRef.get(last), lastPage);
			PageOfRef newPageOfRef = new PageOfRef(Tree, Key, last+1, N);
			String filePath = newPageOfRef.getFilePath();
			pagesOfRef.add(filePath);
			newPageOfRef.refs.add(r);
			serializePageOfRef(filePath, newPageOfRef);
		}
	}
	
	public boolean removeRef(Ref r) throws DBAppException{
		boolean removed = false;
		if(pagesOfRef.size()==0)
			return false;
		for (int i = 0; i < pagesOfRef.size(); i++) {
			boolean found = false;
			PageOfRef curr = deserializePageOfRef(pagesOfRef.get(i));
			for (int j = 0; j < curr.refs.size(); j++) {
				if(curr.refs.get(j).equals(r)){
					curr.refs.remove(j);
					found = true;
					if(curr.refs.size()==0){
						deleteFile(pagesOfRef.get(i));
						pagesOfRef.remove(i);
						return true;
					}
					break;
				}
			}
			Ref lastRef;
			if(found && i!=pagesOfRef.size()-1){
				String lastPageOfRefName = pagesOfRef.get(pagesOfRef.size()-1);
				PageOfRef lastPage = deserializePageOfRef(lastPageOfRefName);
				lastRef = lastPage.refs.remove(lastPage.refs.size()-1);
				curr.refs.add(lastRef);
				if(lastPage.refs.size()==0){
					deleteFile(lastPageOfRefName);
					pagesOfRef.remove(pagesOfRef.size()-1);
				}else{
					serializePageOfRef(lastPageOfRefName, lastPage);
				}
				removed = true;
			}
			serializePageOfRef(pagesOfRef.get(i), curr);
		}
		return removed;
	}
	
	public boolean updateRef(Ref newRef, Ref oldRef) throws DBAppException{
		boolean updated = false;
		for (int i = 0; i < pagesOfRef.size(); i++) {
			String curName = pagesOfRef.get(i);
			
			PageOfRef curr = deserializePageOfRef(curName);
			for (int j = 0; j < curr.refs.size(); j++) {
				if(curr.refs.get(j).equals(oldRef)){
					updated = true;
					curr.refs.set(j, newRef);
					serializePageOfRef(curName, curr);
					return true;
				}
			}
			serializePageOfRef(curName, curr);
		}
		return updated;
	}
	
	public boolean deleteEntireGroupOfRef(){
		for (int i = pagesOfRef.size() - 1; i >=0; i--) {
			deleteFile(pagesOfRef.get(i));
			pagesOfRef.remove(i);
		}
		pagesOfRef = null;
		return true;
	}
	
	public boolean isEmpty(){
		return pagesOfRef.size()==0;
	}
	
	public ArrayList<Ref> getAllRefs() throws DBAppException{
		ArrayList<Ref> res = new ArrayList<Ref>();
		for (int i = 0; i < pagesOfRef.size(); i++) {
			String curName = pagesOfRef.get(i);
			PageOfRef curPage = deserializePageOfRef(curName);
			res.addAll(curPage.refs);
		}
		return res;
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
	
	private static void deleteFile(String filename) {
        File f = new File(filename);
        f.delete();
    }
	
	public String toString1() throws DBAppException{
		String s = "";
		s+="New Group Of Refs";
		s+="\n";
		for (int i = 0; i < pagesOfRef.size(); i++) {
			
			PageOfRef p = deserializePageOfRef(pagesOfRef.get(i));
			s += p.toString();
			s+="\n";
			serializePageOfRef(pagesOfRef.get(i), p);
		}
		return s;
	}
	public void printKeys() throws DBAppException {
		for(int i=0;i<pagesOfRef.size();i++) {
			PageOfRef p = deserializePageOfRef(pagesOfRef.get(i));
			System.out.print(p.key+" ");
			serializePageOfRef(pagesOfRef.get(i), p);
			
		}
	}
}
