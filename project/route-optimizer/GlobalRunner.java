import java.util.*;
import java.io.*;

public class GlobalRunner{
    
    public static void main(String args[]) throws Exception{
        long start = System.currentTimeMillis();

        String txt_file = "input2.txt";

        /* 
        it is easier to work with building names as an integer name,
        rather than their formal names, so create and index so we can
        get back to their formal name later
        */
        HashMap<Integer, String> index = buildIndex(txt_file);

        // read the file in and save it to a HashMap
        HashMap<Integer, HashMap<Integer, Integer>> inFile = readFile(txt_file);

        // create a list of the buildings
        Integer[] iterJob = inFile.keySet().toArray(new Integer[inFile.size()]);
        
        // convert the list to string        
        List<String> lsJob = new ArrayList<String>();
        int i=0;
        for (Integer b : iterJob){
            //skip bldg 0 from the permutation list
            if(b > 0){
                lsJob.add(Integer.toString(b));
                i++;
            }
        }
        /*
        calc all possible combinations of the buildings other 
        than the first/last building in the route

        Building0 is constant, always at the start and end of the route
        */
        List<List<String>> allPerms = generatePerm(lsJob);

        int numThreads = iterJob.length - 1; // num threads must be < num buildings

        // split up the job to numThreads jobs
        int jobsPerThread = allPerms.size() / numThreads;
        routeTimer[] theWorkers = new routeTimer[numThreads];
        // start up all the jobs
        for (i=0; i<=numThreads-1; i++){
            // slice the list of possible routes
            // send each slice to a worker

            int sliceStart = i*jobsPerThread;
            int sliceEnd = sliceStart + jobsPerThread;
            if (sliceEnd >= numThreads){
                sliceEnd = allPerms.size();
            }

            theWorkers[i] = new routeTimer(allPerms.subList(sliceStart,sliceEnd), inFile);
            theWorkers[i].start();
        }

        HashMap<String, Integer> allResults = new HashMap<String, Integer>();
        //collect the results from the workers
        for(routeTimer rt : theWorkers){

            if(rt.isAlive()){
                rt.join();
                allResults.putAll(rt.getMap());
            }
            else{
                allResults.putAll(rt.getMap());
            }
        }

        // each worker returns the fastest route on the worker
        // but need the fastest from all workers
        
        HashMap<String, Integer> fastest = getFastest(allResults);

        //print out results and save to file
        printResults(fastest, index);

    }// end main

    public static void printResults(
        HashMap<String, Integer> hmap,
        HashMap<Integer, String> bldgIndex) throws IOException{
        String key = String.join("",hmap.keySet());
        String[] routeRaw = key.split(":");
        ArrayList<String> routeString = new ArrayList<String>();
        /* 
        so far we've been working with the building names
        according to the integer assigned to them, ie. bldgA is 0.
        Now we need to turn them back to their formal names.
        */
        for (String buildNum : routeRaw){
            // lookup each integer and get the formal name for the bldg
            routeString.add(bldgIndex.get(Integer.parseInt(buildNum)));
        }
        // build the string to print/save to file
        String outString = "Fastest Route:\n" + 
            String.join(" => ",routeString) + "\n" + 
            "Total Time: " + 
            hmap.get(key);
        // then print and save it
        System.out.println(outString);
        File outfile = new File("output2.txt");
        PrintWriter writer = new PrintWriter(outfile, "UTF-8");
        writer.println(outString);
        writer.close();
    }

    public static HashMap<String, Integer> getFastest(HashMap<String, Integer> allTimes){
        String minKey = null;
        int minValue = Integer.MAX_VALUE;
        HashMap<String, Integer> fastest = new HashMap<String, Integer>();
        List<String> keys = new ArrayList<String>(allTimes.keySet());
        
        for(String key : keys) {
                int value = allTimes.get(key);
                if(value < minValue) {
                    minValue = value;
                    minKey = key;
                }
            }
        
        fastest.put(minKey, allTimes.get(minKey));
        return fastest;
    }

    public static HashMap<Integer, HashMap<Integer, Integer>> readFile(String txt_file) throws Exception{

        File file = new File(txt_file); 
        Scanner sc = new Scanner(file);

        HashMap<Integer, String> index = new HashMap<Integer, String>();
        HashMap<Integer, HashMap<Integer, Integer>> inFile = new HashMap<Integer, HashMap<Integer, Integer>>();

        String bldg;

        Integer row = 0;
        while (sc.hasNextLine()) {
            String[] line = sc.nextLine().split(" : ");

            // assign (rownumber: building name) to our index
            index.put(row, line[0]);
            inFile.put(row, new HashMap<Integer, Integer>());

            String[] dist_str = line[1].split(" ");
            int i = 0;
            for (String num : dist_str){
                inFile.get(row).put(i, Integer.parseInt(num));
                i++;
            }
            row++;

            //System.out.println(bldg + "---" + dists);

        } //end while read in 

        return inFile;
    } //end readFile

    public static HashMap<Integer, String> buildIndex(String txt_file) throws Exception{

        File file = new File(txt_file); 
        Scanner sc = new Scanner(file);

        HashMap<Integer, String> index = new HashMap<Integer, String>();

        Integer row = 0;
        while (sc.hasNextLine()) {
            String[] line = sc.nextLine().split(" : ");

            // assign (rownumber: building name) to our index
            index.put(row, line[0]);

            row++;

        } //end while read in 

        return index;
    } //end buildIndex

    public static <E> List<List<E>> generatePerm(List<E> original) {
        //source: https://github.com/viatra/EMF-IncQuery-Addons
     if (original.size() == 0) {
       List<List<E>> result = new ArrayList<List<E>>(); 
       result.add(new ArrayList<E>()); 
       return result; 
     }
     E firstElement = original.remove(0);
     List<List<E>> returnValue = new ArrayList<List<E>>();
     List<List<E>> permutations = generatePerm(original);
     for (List<E> smallerPermutated : permutations) {
       for (int index=0; index <= smallerPermutated.size(); index++) {
         List<E> temp = new ArrayList<E>(smallerPermutated);
         temp.add(index, firstElement);
         returnValue.add(temp);
       }
     }
     return returnValue;
    }

}//end class
