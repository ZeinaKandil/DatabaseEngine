package Nenenenene;

import java.io.Serializable;

public class RefForRTree implements Serializable, Comparable<RefForRTree>{
	public DBPolygon polygon;
	public String pageName;
	public int  indexInPage;
	
	public RefForRTree(String pageName, int indexInPage, DBPolygon poly) {
		this.pageName = pageName;
		this.indexInPage = indexInPage;
		polygon = poly;
	}
	
	public boolean equals(RefForRTree r){
		return r.indexInPage == this.indexInPage && r.pageName.equals(this.pageName) && this.polygon.equals(r.polygon); 
	}
	
	public String toString(){
		return "Page Name " + pageName + " Index " + indexInPage ;
//		return "Page Name " + pageName + " Index " + indexInPage + " Polygon " + polygon;
	}

	@Override
	public int compareTo(RefForRTree r2) {
		if(this.pageName.compareTo(r2.pageName) > 0) return 1;
		if(this.pageName.compareTo(r2.pageName) < 0) return -1;
		if(this.indexInPage > r2.indexInPage) return 1;
		if(this.indexInPage < r2.indexInPage) return -1;
		return 0;
	}

}
