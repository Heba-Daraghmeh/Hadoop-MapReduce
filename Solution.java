
import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 *
 * @author Heba Abderrazeq
 */
public class ass1 {

    /**
     * @param args the command line arguments
     */
   
    public static class UsersMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	
        	String st = value.toString().replaceAll(" ", "");
			String[] str = st.split(",");
            {
            	String ID = str[1].trim();    
            	String val = new String("");
            	if (Integer.parseInt(str[3]) > 25) {
            		
            		val = "U," + str[2] + "," + str[3];
            	}
            	// key --> user id
            	// val ---> u + gender + age
                context.write(new Text(ID),new Text(val));  
            }

        }// end method map

    }// end class UsersMapper

//-------------------------------------------------------------------------------------------
    
    public static class RatingsMapper
    		extends Mapper<LongWritable, Text, Text, Text> {
    	@Override	
    	public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
    			{
    				String st = value.toString().replaceAll(" ", "");
    				String[] str = st.split(",");
    	
    				String ID = str[1].trim();   
    				String val = new String("");
    				if (Integer.parseInt(str[3]) > 2) {
    		
    					val = "R," + str[2] + "," + str[3];
    				}
    				// key --> user id
    				// val --> R + movie id + rating
    				context.write(new Text(ID),new Text(val));  

    			}// end for each
    
	}// end method reduce

    }// end class UsersJoinRatings

    
//-------------------------------------------------------------------------------------------

    public static class UsersJoinRatings
            extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            boolean age25 = false, rating2 = false;
            for (Text value : values) {

            	String[] str = value.toString().split(",");
            	
                if (str[0].equals("U")) {
                    age25 = true;
                } else if (str[0].equals("R")) {
                    rating2 = true;
                }
                if (age25 && rating2){
                	break;
                }

            }// end for each

            if (age25 && rating2) {
               for (Text value : values) {
            	   	 
            	   String[] str = value.toString().split(",");
            	   
                    if (str[0].equals("R")) {
                        String movieID = str[1];
                        Text T = new Text(movieID);
                        String rating = str[2];
                        String val = rating;
                        context.write(T ,new Text(val));   // (movieID,  pair: [R + Rating)] )
                        // KEYOUT:=   movieID, VALUEOUT:= rating
                    }// end if

                }// end for each
            }

        }// end method reduce

    }// end class UsersJoinRatings

    public static class MoviesMapper
            extends Mapper<LongWritable, Text, Text, Text> {// KEYIN, VALUEIN, KEYOUT, VALUEOUT

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
        	
        	String st = value.toString().replaceAll(" ", "");
			String[] str = st.split(",");
       		if (str[3].trim().equalsIgnoreCase("children") || str[3].trim().equalsIgnoreCase("comedy")) {
       			
       			context.write(new Text(str[1]), new Text("M," + str[2]));
       			// key:= movieID,  value:= pair ["M, title]
        	}
        }
    }
    
    
    	public static class SecMapper extends Mapper<LongWritable, Text, Text, Text> {
		
    		@Override
    		public void map(LongWritable key, Text value, Context context)
    				throws IOException, InterruptedException {
			
    			String[] str = value.toString().split("\t");
    			context.write(new Text(str[0]), new Text("R," + str[1]));
    			// movie id , r + rating
    		}
    	}

    

    public static class MoviesReducer
            extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            boolean foundM = false, foundR = false;
            float sum = 0f;
            int cnt = 0;
            String strTitle = "";
            for (Text val : values) {
            	String st = val.toString().replaceAll(" ", "");
    			String[] str = st.split(",");
                if (str[0].equals("M")) {
                    foundM = true;
                    strTitle =  str[1]; // store the title
                } else if (str[0].equals("R")) {
                    foundR = true;
                    sum += Float.parseFloat(str[1]);// the rating
                    cnt += 1;
                }
            }

            if (foundM && foundR) {
                String finalOutput = strTitle + "," + (sum/cnt);
                context.write(key,new Text(finalOutput));
                 //movieID is the key of the reducer   
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
      	Configuration conf = new Configuration();
    	
        Job job = Job.getInstance(conf, "ass");
        job.setJarByClass(ass1.class);
        
        job.setMapperClass(UsersMapper.class);
        job.setMapperClass(RatingsMapper.class);
        
        job.setReducerClass(UsersJoinRatings.class);
        
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, UsersMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingsMapper.class);
       
         
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
      
        
        /* job 2 */
        Configuration conf2 = new Configuration();

        Job job2 = Job.getInstance(conf2, "ass1"); 
        job2.setJarByClass(ass1.class);
        
        job2.setMapperClass(MoviesMapper.class);
        job2.setMapperClass(SecMapper.class);
        
        job2.setReducerClass(MoviesReducer.class);
        
        MultipleInputs.addInputPath(job2, new Path(args[3]), TextInputFormat.class, MoviesMapper.class);
        MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, SecMapper.class);
        
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        
        FileOutputFormat.setOutputPath(job2, new Path(args[4]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    	
    }// end main */

}

