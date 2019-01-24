
import java.io.IOException;
import java.util.*;


/**
 * @author Huang Hao(hh1819) & Lin Dongxu (dl3636)
 * @date 07/12/2018
 * Transaction manager is to process the instruction line by line, and manage different kinds of instrucion,
 * it handle the instruction from read file and waiting list. It also do deadlock detection.
 */
public class TransactionManager {
	Parser parser;
	Map<String, Transaction> Trans;
	SiteManager SM;
	boolean cycleCheckFlag;
	String cycleCheckTranName;
	int timestamp;

    /**
     * this is for standard input
     * @throws IOException, when input file has problem
     */
	public TransactionManager() throws IOException{
		this.parser = new Parser();
		this.SM = new SiteManager();
		this.Trans = new HashMap<>();
	}

    /**
     * this is constructor for file input
     * @param filename, input file directory
     * @throws IOException,  when input file has problem
     */
	public TransactionManager(String filename) throws IOException{
		this.parser = new Parser(filename);
		this.SM = new SiteManager();
		this.Trans = new HashMap<>();
		cycleCheckFlag = false;
		cycleCheckTranName = "";
	}

    /**
     * run() is the method for handling different type of instruction line by line
     * @throws Exception when the instruction is illegal
     */
	public void run() throws Exception {
		timestamp = 0;
		Transaction tran;
		while(parser.hasNext()){
			timestamp++;
			// System.out.println(parser.getNext());
			String line = parser.getNext();
			Instruction inst = new Instruction(line, timestamp);
//			System.out.println(inst);
			if (cycleCheckFlag){
			    cycleCheck(Trans.get(cycleCheckTranName));
			    cycleCheckTranName = "";
			    cycleCheckFlag = false;
            }
			switch(inst.type){

			case begin:
				tran = new Transaction(inst.transName, timestamp, false);
				this.Trans.put(inst.transName, tran);
				break;
			case beginRO:
				tran = new Transaction(inst.transName, timestamp, true);
				this.Trans.put(inst.transName, tran);
				break;
			case R:
				//
				tran = this.Trans.get(inst.transName);
				inst.tran = tran;
				tran.instList.add(inst);
				processReadInst(inst, false);

				break;
			case W:
				// if success, add to changes list
                tran = this.Trans.get(inst.transName);
                inst.tran = tran;
                tran.instList.add(inst);
                processWriteInst(inst, false);
				break;
			case fail:

				// get all trans that can be affected, abort them
//                Site failSite = SM.sites[Integer.valueOf(inst.siteID)];
//                failSite.status = SiteStatus.Down;
//                Map<String, List<String>> tranMap = failSite.lockHolders;
//
//                List<Transaction> abortTran = new LinkedList<>();
//                for (List<String> lst: tranMap.values()){
//                    if (!lst.isEmpty()){
//                        for (String tranName: lst){
//                            abortTran.add(Trans.get(tranName));
//                        }
//                    }
//                }
				List<String> abortTran = SM.fail(inst.siteID, timestamp);
                for (String t: abortTran){
                	tran = this.Trans.get(t);
                    abort(tran);
                }
				break;
			case recover:
//                Site recoverSite = SM.sites[Integer.valueOf(inst.siteID)];
//                recoverSite.status = SiteStatus.Up;
//
				SM.recover(inst.siteID, timestamp);
				int id = inst.siteID;
                if (id % 2 == 0){
                    List<Instruction> instList1 = SM.waitingList.get("x" + (id - 1));
                    List<Instruction> instList2 = SM.waitingList.get("x" + (id + 9));
                    if (instList1 != null) processWaitingList(instList1);
                    if (instList2 != null) processWaitingList(instList2);
                }

				break;
			case dump:
				//
                System.out.println("");
				SM.dump(inst);
				break;
			case end:
				// check if transaction can commit
				// tran is not aborted, not waiting 
				// can simply skip if read-only
				// then release lock, process waiting list

				tran = this.Trans.get(inst.transName);
				inst.tran = tran;

				if (tran.status.equals(TransStatus.active)){
				    SM.commit(tran.changes, timestamp);
                    System.out.println(tran.name + " commits");
				    for (String varName: tran.holdingLocks.keySet()) {
                        releaseLock(varName, tran);
                        List<Instruction> instList = SM.waitingList.get(varName);
                        if (instList != null) processWaitingList(instList);
                    }
                } else if(tran.status.equals(TransStatus.waiting)){
                	abort(tran);
                }

				break;
			default:
				System.out.println("Invalid instruction type");
				throw new Exception();
			}
		}
	}

	private boolean processReadInst(Instruction inst, boolean fromWaitingList){
        Transaction tran = this.Trans.get(inst.transName);
        inst.tran = tran;
        if(tran.status != TransStatus.abort){
            if(tran.isReadOnly){
                // multiversion read
                // check read only available then do it.
                // if false, which means that site is down
                // go to waiting list site manager and set transaction
                if(SM.checkReadOnly(inst.varName, tran.timestamp)){
                    int val = SM.read(tran, true, inst.varName);
                    System.out.println(inst.toString() + "  " + val);
                }
                else{
                    if (!fromWaitingList){
                        putInstToWaiting(tran, inst);
                        cycleCheckFlag = true;
                        cycleCheckTranName = tran.name;
                    }
                }
            }
            else{
                // check if tran has read or write lock on that variable
                // get read lock
                // if true, set read lock
                // if false, go to waiting list site manager and set transaction.
                // do cycle check
                if (tran.holdingLocks.containsKey(inst.varName)){
                    int val = SM.read(tran, false, inst.varName);
                    System.out.println(inst.toString() + val);
                }else{
                    if (SM.getReadLock(tran.name, inst.varName)){
                        int val = SM.read(tran, false, inst.varName);
                        tran.holdingLocks.put(inst.varName, Lock.Read);
                        System.out.println(inst.toString() + " " + val);
                    }else{
                        if (!fromWaitingList){
                            putInstToWaiting(tran, inst);
                            cycleCheckFlag = true;
                            cycleCheckTranName = tran.name;
                        }
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private boolean promoteOrGetWriteLock(Instruction inst, Transaction tran, boolean fromWaitingList){
        if (SM.getWriteLock(tran.name, inst.varName)){
            tran.changes.put(inst.varName, inst.value);
            tran.holdingLocks.put(inst.varName, Lock.Write);
            return true;
        }else{
            if (!fromWaitingList) {
                putInstToWaiting(tran, inst);
                cycleCheckFlag = true;
                cycleCheckTranName = tran.name;
            }
            return false;
        }
    }

    private boolean processWriteInst(Instruction inst, boolean fromWaitingList){
        // if success, add to changes list

        Transaction tran = Trans.get(inst.transName);
        inst.tran = tran;
        if(tran.status != TransStatus.abort){
            // check if already hold lock

            if (tran.holdingLocks.containsKey(inst.varName)){
                Lock lo = tran.holdingLocks.get(inst.varName);
                if (lo.equals(Lock.Write)){
                    tran.changes.put(inst.varName, inst.value);
                    return true;
                }else if (lo.equals(Lock.Read)){
                    return promoteOrGetWriteLock(inst, tran, fromWaitingList);
                }
            }else{
                return promoteOrGetWriteLock(inst, tran, fromWaitingList);
            }
        }
        return false;
    }


//    public void processWaitingList(List<Instruction> instList){
//        Iterator it = instList.iterator();
//
//        for (Instruction inst: instList){
//            if (inst.tran.status != TransStatus.abort){
//                if (inst.type == InstType.W){
//                    if (processWriteInst(inst)){
//                        SM.waitingList.get(inst.varName).remove(inst);
//                    }
//                }else if (inst.type == InstType.R){
//                    if (processReadInst(inst)){
//                        SM.waitingList.get(inst.varName).remove(inst);
//                    }
//                }
//                inst.tran.clearHold();
//            }
//        }
//    }

    public void processWaitingList(List<Instruction> instList){
        Iterator<Instruction> it = instList.iterator();

        while (it.hasNext()){
            Instruction inst = (Instruction)it.next();
            if (inst.tran.status != TransStatus.abort){
                if (inst.type == InstType.W){
                    if (processWriteInst(inst, true)){
                        it.remove();
                    }
                }else if (inst.type == InstType.R){
                    if (processReadInst(inst, true)){
                        it.remove();
                    }
                }
                inst.tran.clearHold();
            }
        }
    }




    public void releaseLock(String varName, Transaction tran){
        // release lock, one or more depending on write or read and process waiting list
        if (Integer.valueOf(varName.substring(1)) % 2 == 0){
            for (int i = 1; i <= 10; i++){
                SM.sites[i].remove(varName, tran);
            }

        }else{
            SM.sites[Integer.valueOf(varName.substring(1))%10 + 1].remove(varName, tran);
        }
        List<Instruction> instList = SM.waitingList.get(varName);

        if (instList != null) processWaitingList(instList);
    }

	private void putInstToWaiting(Transaction tran, Instruction inst){
        tran.status = TransStatus.waiting;
        if (!SM.waitingList.containsKey(inst.varName)){
            SM.waitingList.put(inst.varName, new LinkedList<>());
        }
        List<Instruction> temp = SM.waitingList.get((inst.varName));

        temp.add(inst);
        tran.block(inst);
    }
	
	private void cycleCheck(Transaction trans){
		// DFS cycle check start from variableID
		// if found, abort transaction, clean waiting list.
		Set<String> st = new HashSet<>();
		boolean flag = cycleCheckHelper(trans.name, trans.waitingInst.varName, st);
		if(flag){
			// get youngest transaction according to timestamp in the set
			// abort that transaction
            int ts = -1;
            Transaction tran = null;
            for (String tranName: st){
                if (ts < Trans.get(tranName).timestamp){
                    ts =  Trans.get(tranName).timestamp;
                    tran = Trans.get(tranName);
                }
            }
            if (tran != null) abort(tran);
		}
	}
	
	private void abort(Transaction tran){
		// kill trans, release locks, clear waiting list in the site manager
        if (!tran.status.equals(TransStatus.abort)) {
            tran.status = TransStatus.abort;
            System.out.println(tran.name + " aborts");
            for (String varName : tran.holdingLocks.keySet()) {
                releaseLock(varName, tran);
                for (String str : tran.holdingLocks.keySet()) {
                    for (Instruction inst : tran.instList) {
                        if (SM.waitingList.get(str) != null) {
                            SM.waitingList.get(str).remove(inst);
                        }
                    }

                }

                List<Instruction> instList = SM.waitingList.get(varName);
                if (instList != null) processWaitingList(instList);
            }
        }

    }
	
	private boolean cycleCheckHelper(String transName, String varName, Set<String> st){
		// trans is waiting for variable
		if(st.contains(transName)){
			// find cycle
			return true;
		}
		else {
			st.add(transName);
			List<String> varHolder = SM.getLockHoldingList(varName);
			for(String vh : varHolder){
			    if (!vh.equals(transName)){
                    Transaction h = Trans.get(vh);
                    if(h.waitingInst != null){
                        boolean flag = cycleCheckHelper(h.name, h.waitingInst.varName, st);
                        if(flag) return true;
                    }
                }
			}
			st.remove(transName);
			return false;
		}
	}
}
