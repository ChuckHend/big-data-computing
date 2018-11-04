//This import statement is needed to use the Scanner.
import java.util.*;
import java.io.*;

public class Second{
    public static void main(String args[]) throws Exception{

        ArrayList<Integer> ints = readIntFile("input.txt");

        printMedian(ints);

    }

    public static ArrayList<Integer> readIntFile(String infile) throws Exception{
            File file = new File(infile); 
            Scanner sc = new Scanner(file);

            ArrayList<Integer> myList = new ArrayList<Integer>();
            //extract each integer from the scanner, append to our array
            while (sc.hasNextInt()) 
                myList.add(sc.nextInt());
            
            return myList;
    }//end readFile

    public static void printMedian(ArrayList<Integer> ints){
        Collections.sort(ints);
        int median;
        //compute median
        if (ints.size() % 2 == 0){
            median = (ints.get(ints.size()/2) + ints.get(ints.size()/2 - 1))/2;
        }
        else{
            median = ints.get(ints.size()/2);
        }

        System.out.println(median);
    }//end readFile
}
