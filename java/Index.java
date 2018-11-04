import java.util.*;
import java.io.*;

public class Index{

    
    public static void main(String args[]) throws Exception{
        long start = System.currentTimeMillis();

        TreeMap<String, HashSet<String>> wordIndex = new TreeMap<String, HashSet<String>>();

        Integer pageLen = Integer.parseInt(args[2]);

        File[] inFiles = new File(args[0]).listFiles();
        String outFolder = args[1];     //output folder

        //read in first file
        for(File file : inFiles){
            //read in the file
            Scanner scan = new Scanner(file);
            String content = scan.useDelimiter("\\Z").next(); // read entire file to string
            
            String words[] = content.split("\\r?\\n|\\t|\\s+"); //newline, tab, space delimit
            Integer page = 1; // start at page 1
            Integer page_words = 0;

            for(String word : words){
                //force upper case comparisions
                word = word.toLowerCase();
                int length = word.length(); // count number of chars in the word
                
                //determine which page we are on
                if (page_words + length > pageLen){
                    //increment the page if we push over 100 chars
                    page += 1;
                    //then reset the number of words on page to current word
                    page_words = length;
                }
                else {
                    // if <= 100, increase page words by word
                    page_words += length;
                    // do not change page number
                }
                //add the word to hashmap if not exist
                if (wordIndex.get(word) == null) {
                    wordIndex.put(word, new HashSet<String>());
                }
                //add the page number to the map
                String strPage = String.valueOf(page);
                wordIndex.get(word).add(strPage);
            }
            String outPath = file.getName();
            outPath = outPath.replace(args[0],"").replace(".txt", "_output.txt");
            outPath = outFolder + "/" + outPath;

        //output it
        saveHash(wordIndex, outPath);

        }
    long runTime = System.currentTimeMillis()-start;
    System.out.println(+runTime);
    }

    public static void saveHash(TreeMap<String, HashSet<String>> myTree, String outPath) 
        throws IOException{
        File file = new File(outPath);
        file.getParentFile().mkdirs();
        PrintWriter writer = new PrintWriter(file, "UTF-8");
        for(Map.Entry<String, HashSet<String>> entry : myTree.entrySet()) {
            String key = entry.getKey();
            //let's exclude the null keys
            if (key.length() > 0){
                HashSet<String> val = entry.getValue();
                String outStr = String.join(", ", val);
                String fileOut = key + " " + outStr;
                writer.println(fileOut);
            }
        }
        writer.close();
    }
}
