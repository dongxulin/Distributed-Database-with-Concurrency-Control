
import java.util.*;

/**
 * A variable object is either replicated or unreplicated, it has many version for multi-version read
 * @author Huang Hao(hh1819) & Lin Dongxu (dl3636)
 * @date 07/12/2018
 */
public class Variable {
	String name;
	boolean isReplicated;
	// multiversion read 
	List<Integer> values = new ArrayList<>();
	List<Integer> timestamp = new ArrayList<>();
	
	Status status = Status.OK; 
	

	public Variable(String name, int value, boolean isRep) {
		this.name = name;
		this.isReplicated = isRep;
		this.values.add(value);
		this.timestamp.add(-1);
	}

	/**
	 * this is for multi version read
	 * @param ts, time stamp
	 * @return the value
	 */
	public int multiRead(int ts){
		int pos = timestamp.size() - 1;
		while(timestamp.get(pos) > ts){
			pos--;
		}
		return values.get(pos);
	}
	
	public int read(){
		return values.get(values.size()-1);
	}
	
	public void update(int value, int ts){
		this.status = Status.OK;
		this.values.add(value);
		this.timestamp.add(ts);
	}

	@Override
    public String toString(){
		int v = values.get(values.size()-1);
		return name + ":" + v;
	}
}
