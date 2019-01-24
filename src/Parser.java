
import java.io.File;
import java.io.IOException;
import java.util.Scanner;

/**
 * parser object is to parse the input file
 * @author Huang Hao(hh1819) & Lin Dongxu (dl3636)
 * @date 07/12/2018
 */
public class Parser  {
	private Scanner sc;
	String line;
	boolean flag = false;
	// Instruction Inst;
	
	public Parser() throws IOException {
		System.out.print("Input file: ");
		Scanner reader = new Scanner(System.in);
		String filename = reader.nextLine().trim();
		if(filename.isEmpty() || filename == ""){
			sc = new Scanner(System.in);
			flag = true;
		} else {
			File file = new File(filename);
			sc = new Scanner(file);
		}
	}
	
	public Parser(String filename) throws IOException {
		File file = new File(filename);
		sc = new Scanner(file);
	}
	
	public boolean hasNext() {
		line = "";
		String l = null;
		while(flag || sc.hasNextLine()){
			l = sc.nextLine();
			if(l.isEmpty() || l.startsWith("//")) continue;
			else {
				int pos = l.indexOf("//");
				if(pos == -1){
					line = l;					
				} else {
					line = l.substring(0, pos);
				}
				break;
			}			
		}
		// System.out.println(line);
		return !line.isEmpty();
	}
	
	public String getNext() {
		return line;
	}
}
