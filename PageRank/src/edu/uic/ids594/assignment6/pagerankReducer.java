package edu.uic.ids594.assignment6;

import java.io.IOException;
import java.util.Iterator;
import java.util.Scanner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class pagerankReducer extends MapReduceBase
implements Reducer<Text, Text, Text, Text>{

	@Override
	public void reduce(Text key, Iterator<Text> value,
			OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {

		double pageRank=0.0;
		String pageString="";
		//System.out.println("Reducer Output");
		while(value.hasNext()){
			
			String tempVal = value.next().toString();
			Scanner scanner = new Scanner(tempVal);
			
			if(scanner.hasNextDouble()){
				pageRank= pageRank+Double.parseDouble(tempVal);
			}else{
				pageString=tempVal;
			}
			
		}
		
		collector.collect(new Text(key.toString()+","+pageRank), new Text(pageString));
		//System.out.println(key.toString()+","+pageRank+"---->"+ pageString);
	}

}