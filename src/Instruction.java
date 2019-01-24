

/**
 * An instruction object has field of transaction, type, time stamp, varName and so on.
 * @author Huang Hao(hh1819) & Lin Dongxu (dl3636)
 * @date 07/12/2018
 */
public class Instruction {
	Transaction tran;
	int timestamp;
	InstType type;
	String transName;
	String varName = "";
	int value;
	int siteID = 0;
	
	public Instruction(String line, int ts){
		this.timestamp = ts;
		if(line.charAt(0) == 'b'){
			if(line.charAt(5) == '('){
				type = InstType.begin;
			}
			else{
				type = InstType.beginRO;
			}
			int l = line.indexOf('(');
			int r = line.indexOf(')');
			transName = line.substring(l+1, r).trim();
		}
		else if(line.charAt(0) == 'R'){
			type = InstType.R;
			int pos = line.indexOf(',');
			transName = line.substring(2, pos).trim();
			int r = line.indexOf(')');
			varName = line.substring(pos+1, r).trim();
		}
		else if(line.charAt(0) == 'W'){
			type = InstType.W;
			int pos = line.indexOf(',');
			transName = line.substring(2, pos).trim();
			int pos1 = line.indexOf(',', pos+1);
			varName = line.substring(pos+1, pos1).trim();
			int r = line.indexOf(')');
			value = Integer.parseInt(line.substring(pos1+1, r).trim());
		}
		else if(line.charAt(0) == 'd'){
			type = InstType.dump;
			int l = line.indexOf('(');
			int r = line.indexOf(')');
			if(r > l+1){
				String s = line.substring(l+1, r).trim();
				if(s.charAt(0) == 'x'){
					varName = s;
				}
				else {
					siteID = Integer.parseInt(s);
				}
			}
		}
		else if(line.charAt(0) == 'e'){
			type = InstType.end;
			int l = line.indexOf('(');
			int r = line.indexOf(')');
			transName = line.substring(l+1, r).trim();
		}
		else if(line.charAt(0) == 'f'){
			type = InstType.fail;
			int l = line.indexOf('(');
			int r = line.indexOf(')');
			siteID = Integer.parseInt(line.substring(l+1, r).trim());
		}
		else if(line.charAt(0) == 'r'){
			type = InstType.recover;
			int l = line.indexOf('(');
			int r = line.indexOf(')');
			siteID = Integer.parseInt(line.substring(l+1, r).trim());
		}
	}

	@Override
	public String toString(){
		switch(type){

		case begin:
			return "begin "+transName;
		case beginRO:
			return "beginRO "+transName;
		case R:
			return "R "+transName + ' ' + varName;
		case W:
			return "W "+transName + ' ' + varName + ' ' + value;
		case fail:
			return "fail "+siteID;
		case recover:
			return "recover "+siteID;
		case dump:
			String s = "dump";
			if(!varName.isEmpty()){
				s += " " + varName;
			}
			else if(siteID != 0){
				s += " " + siteID;
			}
			return s;
		case end:
			return "end "+transName;
		}
		return "";
	}
}
