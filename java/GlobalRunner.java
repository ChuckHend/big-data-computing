import java.util.*;
import java.io.*;
import java.util.concurrent.*;

public class GlobalRunner{
    
    public static void main(String args[]) throws Exception{
        long start = System.currentTimeMillis();
        //each thread will return a treemap
        TreeMap<String, TreeMap<String, String>> allResults = new TreeMap<String, TreeMap<String, String>>();

        String outFolder = args[1];     //output folder
        Integer pageLen = Integer.parseInt(args[2]);
        File[] inFiles = new File(args[0]).listFiles();


        int numThreads = inFiles.length;
        globalIndex[] theWorkers = new globalIndex[numThreads]; //run it on 6 cores

        //start the workers and build the header file
        ArrayList<String> header = new ArrayList<String>();
        int i=0;
        for(File file : inFiles){
            header.add(file.getName().replace(args[0],""));
            theWorkers[i] = new globalIndex(file, pageLen);
            theWorkers[i].start();
            i++;
        }

        //collect the results
        i = 0;
        for(globalIndex mi : theWorkers){
            //key will be the filename, strip out the path to file
            String key = inFiles[i].getName().replace(args[0],"");
            if(mi.isAlive()){
                mi.join();
                allResults.put(key, mi.getMap());
            }
            else{
                allResults.put(key, mi.getMap());
            }
            i++;
        }

        String outPath = outFolder + "/output.txt";
        String strHeader = "Word, " + String.join(", ", header);

        shuffleSave(allResults, outPath, strHeader);

        long runTime = System.currentTimeMillis()-start;
        System.out.println(+runTime);
    }

    public static void shuffleSave(TreeMap<String, TreeMap<String, String>> allMap, String outPath, String header) 
        throws IOException{
        String results;

        //create map with master set of words
        Set<String> allFile = allMap.keySet();
        TreeSet<String> allWords = new TreeSet<String>();
        
        ArrayList<String> placeHolder = new ArrayList<String>();

        for(String fileN : allFile){
            // get all words from the nth file
            Set<String> fileWords = allMap.get(fileN).keySet();
            // add each word to the master list of words
            for (String w : fileWords){
                allWords.add(w);
            }
        } //now myShuff has keys of all the words we are working with

        File outfile = new File(outPath);
        outfile.getParentFile().mkdirs();
        PrintWriter writer = new PrintWriter(outfile, "UTF-8");

        // build the file header
        header = "Word, " + String.join(", ",allMap.keySet());

        //print header to file
        writer.println(header);
        
        for(String word : allWords){
            String outString;
        
            outString = word + ", ";
            // then over each file
            for(String file : allFile){
                // select each word from mySuff, in each file
                if(allMap.get(file).containsKey(word)){
                    results = allMap.get(file).get(word);
                }
                else{
                // otherwise throw in a comma
                    results = ", ";
                }
                // build the string for the word
                outString = outString + results;

            }// end iterate each file

            writer.println(outString);
        }// end iterate words

        writer.close();
    }//end shuffleSave
}//end class
