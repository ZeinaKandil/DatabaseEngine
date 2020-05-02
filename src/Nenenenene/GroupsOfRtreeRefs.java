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

public class GroupsOfRtreeRefs implements Serializable{
	public ArrayList<String> pagesOfRef;
	int N;
	
	public GroupsOfRtreeRefs(int n){
		N = n;
		pagesOfRef = new ArrayList<String>();
	}
	
	public void addRef(RefForRTree r, String Tree, String Key) throws DBAppException{
		if(pagesOfRef.size()==0){
			PageOfRTreeRefs p = new PageOfRTreeRefs(Tree, Key, 0, N);
			String filePath = p.getFilePath();
			pagesOfRef.add(filePath);
			p.refs.add(r);
			serializePageOfRTreeRefs(filePath, p);
			return;
		}
		int last = pagesOfRef.size()-1;
		PageOfRTreeRefs lastPage = deserializePageOfRTreeRefs(pagesOfRef.get(last));
		if(lastPage.refs.size() < lastPage.n){
			lastPage.refs.add(r);
			serializePageOfRTreeRefs(pagesOfRef.get(last), lastPage);
		}else{
			serializePageOfRTreeRefs(pagesOfRef.get(last), lastPage);
			PageOfRTreeRefs newPageOfRTreeRefs = new PageOfRTreeRefs(Tree, Key, last+1, N);
			String filePath = newPageOfRTreeRefs.getFilePath();
			pagesOfRef.add(filePath);
			newPageOfRTreeRefs.refs.add(r);
			serializePageOfRTreeRefs(filePath, newPageOfRTreeRefs);
		}
	}
	
	public boolean removeRef(RefForRTree r) throws DBAppException{
		boolean removed = false;
		if(pagesOfRef.size()==0)
			return false;
		for (int i = 0; i < pagesOfRef.size(); i++) {
			boolean found = false;
			PageOfRTreeRefs curr = deserializePageOfRTreeRefs(pagesOfRef.get(i));
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
			RefForRTree lastRef;
			if(found && i!=pagesOfRef.size()-1){
				String lastPageOfRTreeRefsName = pagesOfRef.get(pagesOfRef.size()-1);
				PageOfRTreeRefs lastPage = deserializePageOfRTreeRefs(lastPageOfRTreeRefsName);
				lastRef = lastPage.refs.remove(lastPage.refs.size()-1);
				curr.refs.add(lastRef);
				if(lastPage.refs.size()==0){
					deleteFile(lastPageOfRTreeRefsName);
					pagesOfRef.remove(pagesOfRef.size()-1);
				}else{
					serializePageOfRTreeRefs(lastPageOfRTreeRefsName, lastPage);
				}
				removed = true;
			}
			serializePageOfRTreeRefs(pagesOfRef.get(i), curr);
		}
		return removed;
	}
	
	public boolean RemoveOneByOne(DBPolygon p) throws DBAppException{
		boolean removed = false;
		if(pagesOfRef.size()==0)
			return false;
		for (int i = 0; i < pagesOfRef.size(); i++) {
			boolean found = false;
			PageOfRTreeRefs curr = deserializePageOfRTreeRefs(pagesOfRef.get(i));
			for (int j = 0; j < curr.refs.size(); j++) {
				if(curr.refs.get(j).polygon.equals(p)){
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
			RefForRTree lastRef;
			if(found && i!=pagesOfRef.size()-1){
				String lastPageOfRTreeRefsName = pagesOfRef.get(pagesOfRef.size()-1);
				PageOfRTreeRefs lastPage = deserializePageOfRTreeRefs(lastPageOfRTreeRefsName);
				lastRef = lastPage.refs.remove(lastPage.refs.size()-1);
				curr.refs.add(lastRef);
				if(lastPage.refs.size()==0){
					deleteFile(lastPageOfRTreeRefsName);
					pagesOfRef.remove(pagesOfRef.size()-1);
				}else{
					serializePageOfRTreeRefs(lastPageOfRTreeRefsName, lastPage);
				}
				removed = true;
				break;
			}
			serializePageOfRTreeRefs(pagesOfRef.get(i), curr);
		}
		return removed;
	}
	
	public boolean removePolygon(DBPolygon p) throws DBAppException{
		boolean removed = true;
		int count = 0;
		while(removed){
			count++;
			removed = RemoveOneByOne(p);
		}
		if(count>0) return true;
		return false;
	}
	
//	public boolean removePolygon(DBPolygon polygon) throws DBAppException{
//		boolean removed = false;
//		if(pagesOfRef.size()==0)
//			return false;
//		int deleted = 0;
//		int x = pagesOfRef.size()-1;
//		for (int i = x; i >= 0; i--) {
//			deleted = 0;
//			PageOfRTreeRefs p = deserializePageOfRTreeRefs(pagesOfRef.get(i));
//			for (int j = 0; j < p.refs.size(); j++) {
//				if(p.refs.get(j).polygon.equals(polygon)){
//					deleted++;
//					p.refs.remove(j);
//					removed = true;
//				}
//			}
//			if(i == pagesOfRef.size() && p.refs.size()==0){
//				deleteFile(pagesOfRef.remove(i));
//				continue;
//			}
//			if(i == pagesOfRef.size()){
//				serializePageOfRTreeRefs(pagesOfRef.get(i), p);
//				continue;
//			}
//			String last = pagesOfRef.get(pagesOfRef.size()-1);
//			PageOfRTreeRefs lastPage = deserializePageOfRTreeRefs(last);
//			int size = lastPage.refs.size()-1;
//			for (int j = size; j >= 0 && deleted > 0; j--) {
//				deleted--;
//				p.refs.add(lastPage.refs.remove(j));
//			}
//			if(lastPage.refs.size() == 0){
//				deleteFile(last);
//				pagesOfRef.remove(pagesOfRef.size()-1);
//				serializePageOfRTreeRefs(pagesOfRef.get(i), p);
//				continue;
//			}
//			serializePageOfRTreeRefs(pagesOfRef.get(i), p);
//			serializePageOfRTreeRefs(last, lastPage);
//		}
//		return removed;
//	}
	
	public boolean updateRef(RefForRTree newRef, RefForRTree oldRef) throws DBAppException{
		boolean updated = false;
		for (int i = 0; i < pagesOfRef.size(); i++) {
			String curName = pagesOfRef.get(i);
			PageOfRTreeRefs curr = deserializePageOfRTreeRefs(curName);
			for (int j = 0; j < curr.refs.size(); j++) {
				if(curr.refs.get(j).equals(oldRef)){
					updated = true;
					curr.refs.set(j, newRef);
					serializePageOfRTreeRefs(curName, curr);
					return true;
				}
			}
			serializePageOfRTreeRefs(curName, curr);
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
	
	public ArrayList<RefForRTree> getAllRefs() throws DBAppException{
		ArrayList<RefForRTree> res = new ArrayList<RefForRTree>();
		for (int i = 0; i < pagesOfRef.size(); i++) {
			String curName = pagesOfRef.get(i);
			PageOfRTreeRefs curPage = deserializePageOfRTreeRefs(curName);
			res.addAll(curPage.refs);
		}
		return res;
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
	
	private static void deleteFile(String filename) {
        File f = new File(filename);
        f.delete();
    }
	
	public String toString1() throws DBAppException{
		String s = "";
		for (int i = 0; i < pagesOfRef.size(); i++) {
			s+="New RTreePageOfRef ";
			PageOfRTreeRefs p = deserializePageOfRTreeRefs(pagesOfRef.get(i));
			s += p.toString();
			serializePageOfRTreeRefs(pagesOfRef.get(i), p);
			if(i<pagesOfRef.size()-1) s+= "\n";
		}
		return s;
	}
	public static void main(String [] args){
//		PageOfRTreeRefs pr = deserializePageOfRTreeRefs("data/" +"Pols_PRTreeRef_36000000_0"+ ".class");
	}
}
