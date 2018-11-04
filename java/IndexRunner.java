import java.util.*;
import java.io.*;

public class IndexRunner{
    
    public static void main(String args[]) throws Exception{
        long start = System.currentTimeMillis();

        Integer pageLen = Integer.parseInt(args[2]);

        File[] inFiles = new File(args[0]).listFiles();
        String outFolder = args[1];     //output folder

        //one thread per file
        int numThreads = inFiles.length;
        myIndex[] theWorkers = new myIndex[numThreads]; //run it on 6 cores

        int i=0;
        for(File file : inFiles){
            String outPath = file.getName();
            outPath = outPath.replace(args[0],"").replace(".txt", "_output.txt");
            outPath = outFolder + "/" + outPath;
            theWorkers[i] = new myIndex(file, outPath, pageLen);
            theWorkers[i].start();
            i++;
        }
        long runTime = System.currentTimeMillis()-start;
        System.out.println(+runTime);
        
    }

}
