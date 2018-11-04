import java.util.*;
import java.io.*;

public class myIndex extends Thread{
    private Integer page_words = 0;
    private TreeMap<String, HashSet<String>> wordIndex = new TreeMap<String, HashSet<String>>();
    private Integer pageLen = 1000;
    private File file;
    private String outPath;

    public myIndex(File file, String outPath, Integer pageLen) throws Exception{
        this.file = file;
        this.outPath = outPath;
        this.pageLen = pageLen;


        Scanner scan = new Scanner(this.file);
        //read in the file
        
        String content = scan.useDelimiter("\\Z").next(); // read entire file to string
        
        String words[] = content.split("\\r?\\n|\\t|\\s+"); //newline, tab, space delimit
        Integer page = 1; // start at page 1
        

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

        saveHashMap(wordIndex, outPath);
    }

    public static void saveHashMap(TreeMap<String, HashSet<String>> myTree, String outPath) 
        throws IOException{
        File file = new File(outPath);
        file.getParentFile().mkdirs();
        PrintWriter writer = new PrintWriter(file, "UTF-8");
        for(Map.Entry<String, HashSet<String>> entry : myTree.entrySet()) {
            String key = entry.getKey();
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