package Nenenenene;

import java.awt.Dimension;
import java.awt.Polygon;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

public class DBPolygon extends Polygon implements Comparable<DBPolygon> {
	Polygon poly;
	public DBPolygon(Polygon p) {
		poly = p;
	}

	@Override
	public int compareTo(DBPolygon p2) {
//		Dimension dim = poly.getBounds().getSize( );
//		Integer nThisArea = dim.width * dim.height;
//		Dimension dim2 = p2.getBounds().getSize();
//		Integer nOtherArea = dim2.width * dim2.height;
		Integer nThisArea = this.getArea();
		Integer nOtherArea = p2.getArea();
		return nThisArea.compareTo(nOtherArea);
	}
	
	public int getArea(){
		Dimension dim = poly.getBounds().getSize( );
		Integer nThisArea = dim.width * dim.height;
		return nThisArea;
	}
	

	public boolean equals(Object bb){
		DBPolygon b=(DBPolygon)bb;
		int [] x1 = this.poly.xpoints, x2 = b.poly.xpoints, y1 = this.poly.ypoints, y2 = b.poly.ypoints;
		int n = poly.npoints;
		if(n!=b.poly.npoints)
			return false;
		for (int i = 0; i < n; i++) {
			int iterate = i;
			for (int j = 0; j < n; j++) {
				if(x1[j] != x2[iterate] || y1[j] != y2[iterate]){
					break;
				}
				iterate = (iterate+1)%n;
				if(j == n - 1) return true;
			}
		}
		int [] x3 = new int[n], y3 = new int[n];
		for (int i = 0; i < n; i++) {
			x3[i] = x2[n - 1 -i];
			y3[i] = y2[n - 1 -i];
		}
		for (int i = 0; i < n; i++) {
			int iterate = i;
			for (int j = 0; j < n; j++) {
				if(x1[j] != x3[iterate] || y1[j] != y3[iterate]){
					break;
				}
				iterate = (iterate+1)%n;
				if(j == n - 1) return true;
			}
		}
		return false;
	}
	
	public String toString(){
		String s = "{";
		for (int i = 0; i < poly.xpoints.length; i++) {
			s += "(" + poly.xpoints[i] +"," + poly.ypoints[i] +")";
			if(i<poly.xpoints.length - 1) 
				s += " ";
		}
		s+="}~With Area: " + getArea();
		return s;
	}
}
