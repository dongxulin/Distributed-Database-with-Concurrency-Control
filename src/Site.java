
import java.util.*;

/**
 * @author Huang Hao(hh1819) & Lin Dongxu (dl3636)
 * @date 07/12/2018
 */
public class Site {
	int id;
	int upTime;
	SiteStatus status = SiteStatus.Up;
	List<String> Keys;
	Map<String, Variable> variables;

	// varName --> lock type
	Map<String, Lock> Locks = new HashMap<>();

//	varName -- > List<tranName>
	Map<String, List<String>> lockHolders = new HashMap<>();
	
	public Site(int id) {
		this.id = id;
		this.upTime = 0;
		Keys = new ArrayList<>();
		variables = new TreeMap<>();
	}

	/**
	 * this is to initialize the size with correct variable by adding variable into the site
	 * @param id
	 * @param isRep, is replicated data
	 */
	public void addVar(int id, boolean isRep){
		String name = "x" + id;
		int value = 10 * id;
		Keys.add(name);
		variables.put(name, new Variable(name, value, isRep));
		Locks.put(name, Lock.Idle);
		lockHolders.put(name, new ArrayList<>());
	}
	
	public void update(String varName, int value, int ts){
		variables.get(varName).update(value,ts);

	}
	
	public List<String> fail(int ts) {
		this.upTime = ts;
		status = SiteStatus.Down;
		for(Map.Entry<String, Variable> kv : variables.entrySet()){
			kv.getValue().status = Status.Fail;
			Locks.put(kv.getKey(), Lock.Idle);
		}
		List<String> affectList = new ArrayList<>();
		for(Map.Entry<String, List<String>> m : lockHolders.entrySet()){
			if(!m.getValue().isEmpty()){
				affectList.addAll(m.getValue());
				m.getValue().clear();
			}
		}
		return affectList;
	}

	public void remove(String varName, Transaction tran){
		lockHolders.get(varName).remove(tran.name);
		if (lockHolders.get(varName).size() == 0){
			Locks.put(varName, Lock.Idle);
		}
	}
	
	public void recover(int ts){
		this.upTime = ts;
		for(Map.Entry<String, Variable> kv : variables.entrySet()){
			if(kv.getValue().isReplicated){
				kv.getValue().status = Status.Recover;				
			} else {
				kv.getValue().status = Status.OK;
			}
		}
		status = SiteStatus.Up;
	}
	
	public void dump() {
		System.out.print("site "+this.id+" -");
		for(int i=0; i<Keys.size(); i++){
			System.out.print(' ');
			System.out.print(variables.get(Keys.get(i)));
		}
		System.out.print("\n");
	}
	
	public void dump(String varName){
		System.out.print("site "+this.id+" - ");
		System.out.print(variables.get(varName));
		System.out.print("\n");
	}
	
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("site "+id+" - ");
		for(Map.Entry<String, Variable> var : variables.entrySet()){
			builder.append(var.getValue().toString());
			builder.append(' ');
		}
		return builder.toString();
	}
	
}
