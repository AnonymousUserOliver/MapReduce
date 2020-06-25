import java.io.IOException; 

import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Mapper; 

public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String line = value.toString(); 

		if(searchWord(line, "Chicago")){
			context.write(new Text("Chicago"), new IntWritable(1)); 
		}
		else{
			context.write(new Text("Chicago"), new IntWritable(0));
		}

		if(searchWord(line, "Dec")){
			context.write(new Text("Dec"), new IntWritable(1)); 
		}
		else{
			context.write(new Text("Dec"), new IntWritable(0));
		}

		if(searchWord(line, "Java")){
			context.write(new Text("Java"), new IntWritable(1)); 
		}
		else{
			context.write(new Text("Java"), new IntWritable(0));
		}

		if(searchWord(line, "hackathon")){
			context.write(new Text("hackathon"), new IntWritable(1)); 
		}
		else{
			context.write(new Text("hackathon"), new IntWritable(0));
		}
	}

	public static boolean searchWord(String line, String word){
		if(line.toLowerCase().indexOf(word.toLowerCase()) >= 0){
			return true;
		}
		return false; 
	}
}