package Nenenenene;

import java.io.Serializable;
import java.util.Hashtable;

public class Record implements Serializable,Comparable{
     Hashtable<String, Object> row;
     Position position;
     public Record(Hashtable<String,Object> h ,Position p) {
    	 row=h;
    	 position=p;
    	 
     }
     public String toString(){
    	 return row.toString();
     }
	@Override
	public int compareTo(Object o) {
		Record R=(Record) o;
		if(position.i>R.position.i) {
			return 1;
		}
		else {
			if(position.i<R.position.i)
				return -1;
		}
		return 0;
	}
}
