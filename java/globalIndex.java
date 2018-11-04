import java.util.*;
import java.io.*;

public class globalIndex extends Thread{
    private Integer page_words = 0;
    private TreeMap<String, HashSet<String>> wordIndex = new TreeMap<String, HashSet<String>>();
    private Integer pageLen = 1000;
    private File file;

    public globalIndex(File file, Integer pageLen) throws Exception{
        this.file = file;
        this.pageLen = pageLen;

        Scanner scan = new Scanner(this.file);
        //read in the file
        
        String content = scan.useDelimiter("\\Z").next(); // read entire file to string
        
        String words[] = content.split("\\r?\\n|\\t|\\s+"); //newline, tab, space delimit
        Integer page = 1; // start at page 1
        
        for(String word : words){

            //force lower case comparisions
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
    }

    public TreeMap<String, String> getMap() throws IOException{
        TreeMap<String, String> outMap = new TreeMap<String, String>();
        // transform values to appropriate string, and return it
        for(Map.Entry<String, HashSet<String>> entry : wordIndex.entrySet()) {
            String key = entry.getKey();
            //let's exclude the null keys
            if (key.length() > 0){
                HashSet<String> val = entry.getValue();
                String outStr = String.join(":", val);
                outMap.put(key, outStr);
                //System.out.println(key +"-"+ outStr);
            }
        }
        return outMap;
    }
}