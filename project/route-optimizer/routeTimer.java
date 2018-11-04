import java.util.*;
import java.io.*;

public class routeTimer extends Thread{
    private HashMap<String, Integer> results = new HashMap<String, Integer>();
    public routeTimer(
        List<List<String>> routes, 
        HashMap<Integer, HashMap<Integer, Integer>> distances) 
    throws Exception{
        Integer currentBldg;
        Integer nextBldg;
        Integer totalDistance = 0;

        for(List<String> route : routes){
            // start calculating a route
            totalDistance = 0;

            // start at bldg 0
            currentBldg = 0;
            for (String bldg : route){
                nextBldg = Integer.parseInt(bldg);
                //System.out.println("Curr: " + currentBldg + "Next: " + nextBldg);
                totalDistance += distances.get(currentBldg).get(nextBldg);
                currentBldg = nextBldg;
            }
            // then add distance back to bldg 0
            totalDistance += distances.get(currentBldg).get(0);
            String routeString = String.join(":", route);
            // add bldg0 to beginning and end of the route string
            routeString = "0:" + routeString + ":0";
            // place into the map
            results.put(routeString,totalDistance);
        }

        results = getFastest(results);


    }//end main

    public HashMap<String, Integer> getMap() throws IOException{

        return results;
    } //end getter

    public static HashMap<String, Integer> getFastest(HashMap<String, Integer> allTimes){
        String minKey = null;
        int minValue = Integer.MAX_VALUE;
        // returns a map with object the fastest route
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
}//end class