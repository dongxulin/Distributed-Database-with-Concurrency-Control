
import java.util.*;

/**
 * a transaction maintain a map of holding locks to record what lock it is holding, and the write that not
 * yet commit will be store in the changes map.
 * @author Huang Hao(hh1819) & Lin Dongxu (dl3636)
 * @date 07/12/2018
 */

public class Transaction {
	String name;
	int timestamp;
	boolean isReadOnly;
	TransStatus status;
	Instruction waitingInst;
	List<Instruction> instList;
	Map<String, Lock> holdingLocks = new HashMap<>();
	Map<String, Integer> changes = new HashMap<>();
	
	public Transaction(String name, int ts, boolean isRO) {
		this.name = name;
		this.timestamp = ts;
		this.isReadOnly = isRO;
		this.status = TransStatus.active;
		instList = new LinkedList<>();
	}

	/**
	 * block the transaction
	 * @param Inst
	 */
	public void block(Instruction Inst) {
		this.status = TransStatus.waiting;
		this.waitingInst = Inst;
	}

	/**
	 * clear it out of block
	 */
	public void clearHold() {
		this.waitingInst = null;
		this.status = TransStatus.active;
	}
	
	public boolean checkLock(String varName, Lock l){
		if(holdingLocks.containsKey(varName)){
			if(holdingLocks.get(varName).equals(l)){
				return true;
			} else if (holdingLocks.get(varName).equals(Lock.Write)){
				return true;
			}
			return false;
		}
		return false;
	}
	
}
