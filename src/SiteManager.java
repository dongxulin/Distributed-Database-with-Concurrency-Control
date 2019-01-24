
import java.util.*;


/**
 * A site manager is to provide methods for handling site execution, it has reference to all sites, and maintained a
 * waiting list.
 * @author Huang Hao(hh1819) & Lin Dongxu (dl3636)
 * @date 07/12/2018
 */
public class SiteManager {
	Site[] sites;
	Map<String, Integer> VarID = new HashMap<>();
	Map<String, List<Instruction>> waitingList = new HashMap<>();

	// only write inst waiting for read or write insts, or read inst waiting for write inst.

	/**
	 * construct the site manager and initialize all the sites
	 */
	public SiteManager() {
		// initialize sites 
		sites = new Site[Common.numSites+1];
		for(int i=1; i<=Common.numVariables; i++){
			VarID.put("x"+i, i);
			if(i%2 == 1){
				// odd index variable
				int sID = 1 + i % 10;
				if(sites[sID] == null){
					sites[sID] = new Site(sID);
				}
				sites[sID].addVar(i, false);
			}
			else {
				// even index variable
				for(int j=1; j<=Common.numSites; j++){
					if(sites[j] == null){
						sites[j] = new Site(j);
					}
					sites[j].addVar(i, true);
				}
			}
		}
	}

	/**
	 * check if this variable is OK for the read only transaction
	 * @param varName, variable name
	 * @param ts, time stamp
	 * @return true if it is OK to read, false cannot.
	 */
	public boolean checkReadOnly(String varName, int ts) {
		// if one site is available than true
		int id = VarID.get(varName);
		if(id%2 == 1){
			int sID = 1 + id % 10;
			return sites[sID].status == SiteStatus.Up || (sites[sID].status == SiteStatus.Down && sites[sID].upTime > ts);
		}
		else {
			// if any site is up and data is available then true
			for(int i=1; i<=Common.numSites; i++){
				if(sites[i].status == SiteStatus.Up && 
						sites[i].variables.get(varName).status == Status.OK &&
						sites[i].upTime <= ts){
					return true;
				}
			}
			return false;
		}
	}

	/**
	 * get read lock for the transaction
	 * @param transName, transaction name
	 * @param varName, varialbe name
	 * @return true if get lock successfully, false fail to get
	 */
	public boolean getReadLock(String transName, String varName){
		// if trans own write lock, then true
		// else if value only has read lock and no one waiting, then true
		// else false
		// for replicated variable only find the first available site
		int id = VarID.get(varName);
		int sID = id % 2 == 1 ? 1 + id % 10 : getFirstSite(varName);
		if(sID != 0 && checkReadLockFromSite(sID, transName, varName)){
			setReadLock(sID, transName, varName);
			return true;
		}
		return false;
	}
	
	private int getFirstSite(String varName){
		for(int i=1; i<Common.numSites; i++){
			if(sites[i].status == SiteStatus.Up && 
					sites[i].variables.get(varName).status == Status.OK){
				return i;
			}
		}
		return 0;
	}
	
	private boolean checkReadLockFromSite(int sID, String transName, String varName){
		if(sites[sID].status == SiteStatus.Up && 
				sites[sID].variables.get(varName).status == Status.OK){
			Site s = sites[sID];
			if(s.Locks.get(varName) == Lock.Idle){
				return true;
			} else if (s.Locks.get(varName) == Lock.Read){
				if(s.lockHolders.get(varName).contains(transName)){
					return true;					
				} else if (!waitingList.containsKey(varName)){
					return true;
				}else if (waitingList.get(varName).isEmpty()){
				    return true;
                }
				return false;
			} else if(s.Locks.get(varName) == Lock.Write && 
					s.lockHolders.get(varName).get(0) == transName) {
				return true;
			}
			return false;
		} else {
			return false;			
		}
	}

	/**
	 * set read lock for transaction
	 * @param sID, site ID
	 * @param transName, transaction name
	 * @param varName, variable name
	 */
	private void setReadLock(int sID, String transName, String varName){
		if(sites[sID].status == SiteStatus.Up && 
				sites[sID].variables.get(varName).status == Status.OK){
			Site s = sites[sID];
			if(s.Locks.get(varName) == Lock.Idle) {
				s.Locks.put(varName, Lock.Read);
			}
			if(!s.lockHolders.get(varName).contains(transName)){
				s.lockHolders.get(varName).add(transName);
			}
		}
	}

	/**
	 * get write lock for the transaction
	 * @param transName, transaction name
	 * @param varName, varialbe name
	 * @return true if get lock successfully, false fail to get
	 */
	public boolean getWriteLock(String transName, String varName){
		// if trans own write lock, then true
		// if it exclusively own read lock, can promote????? (no one waiting or else)
		// else false
		int id = VarID.get(varName);
		if(id % 2 == 1){
			int sID = 1 + id % 10;
			if(checkWriteLockFromSite(sID, transName, varName)){
				setWriteLock(sID, transName, varName);
				return true;
			}
			return false;
		} else {
			for(int i=1; i<=Common.numSites; i++){
				if(!checkWriteLockFromSite(i, transName, varName)){
				    if (sites[i].status == SiteStatus.Down){
				        continue;
                    }
					return false;
				}
			}
			for(int i=1; i<=Common.numSites; i++){
				setWriteLock(i, transName, varName);
			}
			return true;
		}
	}
	
	private boolean checkWriteLockFromSite(int sID, String transName, String varName){
		if(sites[sID].status == SiteStatus.Up ){
			// && sites[sID].variables.get(varName).status == Status.OK
			Site s = sites[sID];
			if(s.Locks.get(varName).equals(Lock.Idle)){
				return true;
			} else if (s.Locks.get(varName) == Lock.Read && 
					(!waitingList.containsKey(varName) || waitingList.get(varName).isEmpty()) &&
					s.lockHolders.get(varName).size() == 1 && 
					s.lockHolders.get(varName).get(0) == transName){
				// todo: waiting list should be local
				// promote from read lock
				return true;
			} else if (s.Locks.get(varName) == Lock.Write &&
					s.lockHolders.get(varName).get(0) == transName) {
				// already hold write lock
				return true;
			}
			return false;
		} else {
			return false;
		}
	}
	
	private void setWriteLock(int sID, String transName, String varName){
		if(sites[sID].status == SiteStatus.Up) {
			Site s = sites[sID];
			s.Locks.put(varName, Lock.Write);
			// maybe already has lock
			if(!s.lockHolders.get(varName).contains(transName)){
				s.lockHolders.get(varName).add(transName);
			}
		}
	}

	/**
	 * read value for transaction
	 * @param tran transaction
	 * @param isRO, is read only transaction
	 * @param varName, variable name
	 * @return the result value
	 */
	public int read(Transaction tran, boolean isRO, String varName) {
		int id = VarID.get(varName);
		if(id % 2 == 1){
			int sID = 1 + id % 10;
			if(isRO){
				return sites[sID].variables.get(varName).multiRead(tran.timestamp);
			} else {
				return sites[sID].variables.get(varName).read();
			}
		} else {
			for(int i=1; i<=Common.numSites; i++){
				if(sites[i].status == SiteStatus.Up && sites[i].variables.get(varName).status == Status.OK){
					if(isRO){
						if(sites[i].upTime <= tran.timestamp){
							return sites[i].variables.get(varName).multiRead(tran.timestamp);							
						}
					} else {
						return sites[i].variables.get(varName).read();
					}
				}
			}
		}
		return 0;
	}

	/**
	 * get what transaction is holding the variable's lock
	 * @param varName, variable name
	 * @return a list of transaction name
	 */
	public List<String> getLockHoldingList(String varName){
        List<String> res;
        if (Integer.valueOf(varName.substring(1)) % 2 == 1){
            return sites[Integer.valueOf(varName.substring(1)) % 10 + 1].lockHolders.get(varName);
        }else{
            res = new LinkedList<>();
            for (int i = 1; i <= 10; i++){
                for (String str: sites[i].lockHolders.get(varName)){
                    res.add(str);
                }

            }
        }
		return res;
	}

	/**
	 * fail a site
	 * @param siteID
	 * @return a list of transaction that is affected
	 */
	public List<String> fail(Integer siteID, int ts){
		// set site to be down, all variables in it will be unavailable, clean lock
		// return list of trans id that get involved
		return sites[siteID].fail(ts);
	}

	/**
	 * recover a site
	 * @param siteID
	 * @param ts, time stamp
	 */
	public void recover(Integer siteID, int ts){
		sites[siteID].recover(ts);
	}

	/**
	 * commit a transaction
	 * @param changes, a list of changes/write instrution
	 * @param ts, time stamp
	 */
	public void commit(Map<String, Integer> changes, int ts){
		// update variable value and status
		for(Map.Entry<String, Integer> m : changes.entrySet()){
			int id = VarID.get(m.getKey());
			String varName = m.getKey();
			int value = m.getValue();
			if(id % 2 == 1){
				int sID = 1 + id % 10;
				sites[sID].update(varName, value, ts);
			} else {
				for(int i=1; i<=Common.numSites; i++){
					if(sites[i].status == SiteStatus.Up && sites[i].Locks.get(varName) == Lock.Write){
						// pass test6
						sites[i].update(varName, value, ts);
					}
				}
			}
		}
	}

	/**
	 * dump all result
	 * @param inst, instruction
	 */
	public void dump(Instruction inst){
		if(!inst.varName.isEmpty()){
			int id = VarID.get(inst.varName);
			if(id % 2 == 1){
				int sID = 1 + id % 10;
				sites[sID].dump(inst.varName);
			}
			else{
				for(int i=1; i<=Common.numSites; i++){
					sites[i].dump(inst.varName);
				}
			}
		}
		else if(inst.siteID != 0){
			sites[inst.siteID].dump();
		}
		else {
			for(int i=1; i<=Common.numSites; i++){
				sites[i].dump();
			}			
		}
	}



}
