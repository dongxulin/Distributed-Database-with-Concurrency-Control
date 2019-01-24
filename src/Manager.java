
/**
 * this class has the main method to start the program, if there is no input argument, it use standard input, otherwise
 * use file input.
 * Input type : standard input or file input
 * Output : to the console
 * @author Huang Hao(hh1819) & Lin Dongxu (dl3636)
 * @date 07/12/2018
 */

public class Manager {
	public static void main(String[] args) throws Exception{
		TransactionManager tm;
		if(args.length == 0){
			tm = new TransactionManager();
		} else {
			tm = new TransactionManager(args[0]);
		}
		tm.run();
	}
	
}
