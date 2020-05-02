package Nenenenene;

import java.io.Serializable;

public class Ref implements Serializable, Comparable<Ref>{
	
	/**
	 * This class represents a pointer to the record. It is used at the leaves of the B+ tree 
	 */
	public static final long serialVersionUID = 1L;
	public String pageName;
	public int  indexInPage;
	
	public Ref(String pageName, int indexInPage)
	{
		this.pageName = pageName;
		this.indexInPage = indexInPage;
	}
	
	/**
	 * @return the page at which the record is saved on the hard disk
	 */
	public String getPage()
	{
		return pageName;
	}
	
	/**
	 * @return the index at which the record is saved in the page
	 */
	public int getIndexInPage()
	{
		return indexInPage;
	}
	
	public boolean equals(Ref r){
		return r.getIndexInPage() == this.indexInPage && r.getPage().equals(this.pageName); 
	}
	
	public int compareTo(Ref r2){
		if(this.pageName.compareTo(r2.pageName) > 0) return 1;
		if(this.pageName.compareTo(r2.pageName) < 0) return -1;
		if(this.indexInPage > r2.indexInPage) return 1;
		if(this.indexInPage < r2.indexInPage) return -1;
		return 0;
	}
	
	public String toString(){
		return "Page Name " + pageName + " Index " + indexInPage ;
	}
}