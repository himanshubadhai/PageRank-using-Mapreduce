package edu.uic.ids594.assignment6;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PageRankMapper extends MapReduceBase
implements Mapper<LongWritable, Text, Text, Text>{

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {

		// read line by line and split them by tab
		StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
		String a=stringTokenizer.nextToken();
		System.out.println("mapper "+a);
		
		String[] pageSplit1= a.split(",");
		String pages = stringTokenizer.nextToken();
		//System.out.println(pages);
		
		String[] pageSplit2= pages.split(",");
		
		double beta = 0.8;
		
		// prepare the key value pair
		for (int i = 0; i < pageSplit2.length; i++) {
			
			String keyTemp = pageSplit2[i];			
			double valueTemp = (Double.parseDouble(pageSplit1[1])/pageSplit2.length)*beta;
			
			collector.collect(new Text(keyTemp),new Text(valueTemp+""));
		//System.out.println(keyTemp + "  " + valueTemp);
		}
		collector.collect(new Text(pageSplit1[0]),new Text(pages));
		
	}

}
