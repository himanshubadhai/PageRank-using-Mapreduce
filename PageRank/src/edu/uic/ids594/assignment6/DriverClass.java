package edu.uic.ids594.assignment6;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DriverClass extends Configured implements Tool {

	String outputpath = "/Users/himansubadhai/Documents/input/assignment6/output/part-00000";
	String inputfilePath = "/Users/himansubadhai/Documents/input/assignment6/test_input.txt";
	String outputfilePath = "/Users/himansubadhai/Documents/input/assignment6/output";

	public static final double epslon = 0.1;

	@Override
	public int run(String[] args) throws Exception {
		
		double sum = Double.MAX_VALUE;
		int count = 0;
		while (epslon<sum) {
			// creating configuration object for job-1
			JobConf config = new JobConf(getConf(), DriverClass.class);
			config.setJobName("MapReduceJob");

			// setting configuration parameters for job-1
			config.setOutputKeyClass(Text.class);
			config.setOutputValueClass(Text.class);
			config.setMapOutputKeyClass(Text.class);
			config.setMapOutputValueClass(Text.class);

			// setting classes for job-1
			config.setJarByClass(DriverClass.class);
			config.setMapperClass(PageRankMapper.class);
			config.setReducerClass(pagerankReducer.class);
			// config1.setPartitionerClass(PartitionerClass.class);
			// config1.setNumReduceTasks(3);

			// setting input and output file path for job-1
			String tempInput="";
			if (count == 0) {
				tempInput="/Users/himansubadhai/Documents/input/assignment6/test_input.txt";
				System.out.println(tempInput);
			} else {
				tempInput="/Users/himansubadhai/Documents/input/assignment6/output"
						+ (count - 1)+"/part-00000";
				//System.out.println(tempInput);
			}
			FileInputFormat.addInputPath(config, new Path(tempInput));
			FileOutputFormat.setOutputPath(config, new Path(
					"/Users/himansubadhai/Documents/input/assignment6/output"
							+ count));
			JobClient.runJob(config);
			sum = rankDifference(config, inputfilePath, outputpath, count);
			
			count++;
			}

		return 0;
	}

	public double rankDifference(JobConf config, String input, String output,
			int count) {
		
		double sum = 0.0;
		try {
			FileSystem fileSystem = FileSystem.get(config);
			// BufferedReader reader=new BufferedReader(new
			// FileReader(filepath+"part-000000"));
			Path pathinput;
			if(count==0)
			{
				 pathinput = new Path(inputfilePath);
					
			}
			else
			{
				 pathinput = new Path(
						 outputfilePath+(count-1)+"/part-00000");
					
			}
			Path pathOutput = new Path(
					outputfilePath+count+"/part-00000");

			BufferedReader readerInput = new BufferedReader(
					new InputStreamReader(fileSystem.open(pathinput)));
			BufferedReader readerOutput = new BufferedReader(
					new InputStreamReader(fileSystem.open(pathOutput)));

			String line;
			HashMap<String, Double> hashmap = new HashMap<String, Double>();

			while ((line = readerInput.readLine()) != null) {

				//ArrayList<Double> ranklist = new ArrayList<Double>();

				StringTokenizer stringTokenizerInput = new StringTokenizer(line);
				String tokenInput = stringTokenizerInput.nextToken();
				System.out.println(tokenInput);

				String[] pageSplit1 = tokenInput.split(",");
				String inputPage = pageSplit1[0];
				double inputRank = Double.parseDouble(pageSplit1[1]);

				if (!(hashmap.containsKey(inputPage))) {
					// ranklist.add(inputRank);
					hashmap.put(inputPage, inputRank);
				} else {
					// ArrayList<Double> ranklistTemp = hashmap.get(inputPage);
					// ranklistTemp.add(inputRank);
					double tempRank = hashmap.get(inputPage);
					tempRank = Math.abs(tempRank - inputRank);

					hashmap.put(inputPage, tempRank);
				}

			}

			while ((line = readerOutput.readLine()) != null) {
				StringTokenizer stringTokenizerOutput = new StringTokenizer(
						line);
				String tokenOutput = stringTokenizerOutput.nextToken();
				System.out.println(tokenOutput);

				String[] pageSplit2 = tokenOutput.split(",");
				String outputPage = pageSplit2[0];
				double outputRank = Double.parseDouble(pageSplit2[1]);

				if (!(hashmap.containsKey(outputPage))) {
					// ranklist.add(inputRank);
					hashmap.put(outputPage, outputRank);
				} else {
					// ArrayList<Double> ranklistTemp = hashmap.get(inputPage);
					// ranklistTemp.add(inputRank);
					double tempRank = hashmap.get(outputPage);
					tempRank = Math.abs(tempRank - outputRank);
					hashmap.put(outputPage, tempRank);
				}

			}
			
			Iterator<Double> it = hashmap.values().iterator();
			while (it.hasNext()) {
				sum = sum + it.next();
			}
			System.out.println("sum: " + sum);
			return sum;

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return sum;

	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new DriverClass(), args);
		System.exit(res);
	}

}