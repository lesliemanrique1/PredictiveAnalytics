import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Arrays;
import java.util.HashMap; 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TargetedAd extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new TargetedAd(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "TargetedAd");
      job.setJarByClass(TargetedAd.class);
      job.setOutputKeyClass(Text.class);
     /// job.setOutputValueClass(IntWritable.class);
      job.setOutputValueClass(Text.class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
      
      return 0;
   }
   
   
   public static class Map extends Mapper<LongWritable, Text, Text, Text> {
      private Text cat = new Text(); 

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	  //UID's and Categories are seprated by a tab 
    	  String[] line = value.toString().split("\t");
    	  String uid =line[0]; //the user id 
    	  String[] categories = line[1].split(","); //categories are split by commas (,)  
    	  //combinations 
    	  List<Object> list = new ArrayList<Object>();
    	  for(String c : categories){
    		  //add to list
    		  list.add(c); 
    		  
    	  }
    	  
    	  //get subsets
    	  Set<Set<Object>> subsets = getSubsetUsingBitMap(list); 
    	  
    	  //each subset is a key, value is the uid 
    	  //for each subset 
    	  for(Set<Object> sub: subsets){
    		    String s = ""; 
    		    Object[] arr = sub.toArray();
    		    for(int i = 0; i < arr.length; i++){
    		      if(i==arr.length - 1){
    		        s+=arr[i];
    		      }
    		      else{
    		        s+=arr[i]+",";
    		      }
    		    }
    		   
    		   cat.set(s);
    		   context.write(cat, new Text(uid));

    		  }
    	  

      }
      /** Returns all the posible combinations of categories **/ 
      private static Set<Set<Object>> getSubsetUsingBitMap(List<Object> list){
    	   
    	  Set<Set<Object>> result = new HashSet<Set<Object>>();
    	   
    	  int numOfSubsets = 1 << list.size(); //OR Math.pow(2, list.size())
    	 
    	  // For i from 0 to 7 in case of [a, b, c], 
    	  // we will pick 0(0,0,0) and check each bits to see any bit is set, 
    	  // If set then element at corresponding position in a given Set need to be included in a subset. 
    	  for(int i = 1; i < numOfSubsets; i++){ //i=1 to exclude the null set 
    	       
    	   Set<Object> subset = new HashSet<Object>();
    	 
    	   int mask = 1; // we will use this mask to check any bit is set in binary representation of value i.
    	    
    	   for(int k = 0; k < list.size(); k++){
    	     
    	    if((mask & i) != 0){ // If result is !=0 (or >0) then bit is set.
    	     subset.add(list.get(k)); // include the corresponding element from a given set in a subset.
    	    }
    	     
    	    // check next bit in i.
    	    mask = mask << 1;
    	   }
    	    
    	   // add all subsets in final result.
    	   result.add(subset);
    	  }
    	  return result;  
    	 }
   }

   public static class Reduce extends Reducer<Text, Text, Text, Text> {
      
      public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
    	 //hash map of Strings and Longs 
    	  
    	 HashMap<String, Integer> hm = new HashMap<String,Integer>(); 
    	 //for each v in value 
    	 //if same value incrememnt by one 
    	 //make a hash map of all values 
    	 for (Text val : values) {
    		 String valS = val.toString(); 
    		 if(!hm.containsKey(valS)) {
    			 hm.put(valS,1);
    		 	}
    		 else {
    			hm.put(valS, hm.get(valS)+1);
    		 	}
         }
    	 
    	 String res = ""; 
    	 //iterate through hash map and develope a string representation to send out 
    	 for(String k : hm.keySet()){
    		 res+=k+"("+hm.get(k)+")"+",";
    		 
    	 }
    	 
    	 
         context.write(key, new Text(res));
      }
   }
}

