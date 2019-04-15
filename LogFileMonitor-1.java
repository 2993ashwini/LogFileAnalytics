import java.io.BufferedReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import static java.util.stream.Collectors.*;
import static java.util.Map.Entry.*;

public class LogFileMonitor {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		 System.out.println("Starting Log File Monitor job...");
	     
		 String directoryName = args[0];
		 File directory = new File(directoryName);
		 System.out.println("Accessing directory:"+ directoryName);
		 File[] fList = directory.listFiles();
		  
		 HashMap<String,Integer> accessPageCount = new HashMap<String, Integer>();
		 HashMap<String,Integer> accessSessCount = new HashMap<String, Integer>();
		 HashMap<String,Timestamp> accessLastTime = new HashMap<String, Timestamp>();
		 HashMap<String, Timestamp> accessFirstTime = new HashMap<String, Timestamp>();
		 HashMap<String,Long> accessLongestSess = new HashMap<String, Long>();
		 HashMap<String,Long> accessShortestSess = new HashMap<String, Long>();
		 
	        for (File file : fList){
	        	
	            System.out.println("Gathering logs from:" +file.getName());
	            
	            FileInputStream fstream;
				try {
				fstream = new FileInputStream(file);
				BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
				
	            String strLine;
	            int pageCount = 0;
	          
	           
	            /* read log line by line */
	            while ((strLine = br.readLine()) != null)   {
	              /* parse strLine to obtain what you want */
	            	
	              //System.out.println (strLine);
	              String[] logContents = strLine.split("-");               
	             
	              String uri = logContents[3];
	              
	              String[] splitUriURL = uri.split("\"");
	              String[] splitUri = splitUriURL[1].split("/");
	              String userIdRet = "";
	              String userId = "";
	              
	              if(splitUri.length > 3)
	            	  userId = splitUri[3];
	             
	              if(userId.contains("HTTP"))
	              userId = userId.substring(0, userId.indexOf("HTTP") - 1).trim();
	              
	              
	              if(userId != null && userId.length() !=0 && !userId.equals("HTTP") && userId.equals("43a81873")) {
	             
	              
	              String currTime = logContents[2];
	             
	              SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss");
	              Date parsedDateCurr = dateFormat.parse(currTime);
	              Timestamp currtimestamp = new java.sql.Timestamp(parsedDateCurr.getTime());
	              Timestamp prevtimestamp = accessLastTime.get(userId);
	              Timestamp firsttimestamp = accessFirstTime.get(userId);
	              long diff = 0;
	              long sessDiff = 0;
	              
	              if(prevtimestamp != null)
	                 diff = currtimestamp.getTime() - prevtimestamp.getTime();
	              if(firsttimestamp != null)
		                 sessDiff = currtimestamp.getTime() - firsttimestamp.getTime();
		              
	             
	              long minutes = TimeUnit.MILLISECONDS.toMinutes(diff);
	              long session = TimeUnit.MILLISECONDS.toMinutes(sessDiff);
	              
	              
	              //System.out.println(currtimestamp + "  " + prevtimestamp + "  "+ minutes);
	             
	              pageCount++;
	              accessPageCount.put(userId, Math.max(pageCount, accessPageCount.getOrDefault(userId, 0)));
	             
	              if(accessSessCount.get(userId) == null)
	              {   
	            	  accessSessCount.put(userId, 1);
	                  accessLongestSess.put(userId, session);
	                  accessShortestSess.put(userId, session);
	                  accessFirstTime.put(userId, currtimestamp);
	              }

	             
	              if(minutes > 10) {	
	              
	              accessSessCount.put(userId, accessSessCount.get(userId) + 1); 
	              accessFirstTime.put(userId, currtimestamp);
	              accessShortestSess.put(userId,Math.min(session, accessShortestSess.get(userId))); 
	              pageCount = 0;
	              }else {
	            	 accessShortestSess.put(userId,Math.max(session, accessShortestSess.get(userId))); 
	              }
	             
	              accessLongestSess.put(userId,Math.max(session, accessLongestSess.get(userId))); 	 
	              accessPageCount.put(userId,Math.max(pageCount, accessPageCount.get(userId)));
	              
	              accessLastTime.put(userId, currtimestamp);
	             
	              }             
	            }
	            fstream.close();
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		 } 
	            System.out.println(accessSessCount);
	            System.out.println("Reports:");
	            System.out.println("Total unique users:" + accessPageCount.size());
	            System.out.println("Top users:");
	            int n = 5;
	            List<Entry<String, Integer>> greatest = findGreatest(accessPageCount, 5);
	            System.out.println("Top "+n+" entries:");
	            
	            for (Entry<String, Integer> entry : greatest)
	            {
	            	String userIdUnique = entry.getKey();
	                System.out.println(userIdUnique + "   " + accessPageCount.get(userIdUnique) +"  "+ accessSessCount.get(userIdUnique) +"  " +
	                		accessLongestSess.get(userIdUnique) +"   "+ accessShortestSess.get(userIdUnique));
	            }
	            
   }
 private static <K, V extends Comparable<? super V>> List<Entry<K, V>>  findGreatest(Map<K, V> map, int n)
{
     Comparator<? super Entry<K, V>> comparator = 
         new Comparator<Entry<K, V>>()
     {
         @Override
         public int compare(Entry<K, V> e0, Entry<K, V> e1)
         {
             V v0 = e0.getValue();
             V v1 = e1.getValue();
             return v0.compareTo(v1);
         }
     };
     PriorityQueue<Entry<K, V>> highest = 
         new PriorityQueue<Entry<K,V>>(n, comparator);
     for (Entry<K, V> entry : map.entrySet())
     {
         highest.offer(entry);
         while (highest.size() > n)
         {
             highest.poll();
         }
     }

     List<Entry<K, V>> result = new ArrayList<Map.Entry<K,V>>();
     while (highest.size() > 0)
     {
         result.add(highest.poll());
     }
     return result;
}	
        

}


