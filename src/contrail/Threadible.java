package contrail;

import java.io.IOException;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class Threadible extends Configured implements Tool 
{	
	private static final Logger sLogger = Logger.getLogger(Threadible.class);
	
	
	// ThreadibleMapper
	///////////////////////////////////////////////////////////////////////////
	
	private static class ThreadibleMapper extends MapReduceBase 
    implements Mapper<LongWritable, Text, Text, Text> 
	{
		private static int K = 0;
		
		public void configure(JobConf job) 
		{
			K = Integer.parseInt(job.get("K"));
		}
		
		public void map(LongWritable lineid, Text nodetxt,
                OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException 
        {
			Node node = new Node();
			node.fromNodeMsg(nodetxt.toString());

			output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
			reporter.incrCounter("Contrail", "nodes", 1);
			
			List<String> threadpath = node.getThreadPath();
			
			// Tell my neighbors that I intend to split
			if (threadpath != null && threadpath.size() > 0)
			{
				reporter.incrCounter("Contrail", "threadible", 1);
				
				for(String et : Node.edgetypes)
				{
					List<String> nids = node.getEdges(et);
					String dir = Node.flip_link(et);
					
					if (nids != null)
					{
						for (String v : nids)
						{
							if (!v.equals(node.getNodeId()))
							{
					            output.collect(new Text(v), new Text(Node.THREADIBLEMSG + "\t" + dir + ":" + node.getNodeId()));
							}
						}
					}
				}
			}
        }
	}

	// ThreadibleReducer
	///////////////////////////////////////////////////////////////////////////

	private static class ThreadibleReducer extends MapReduceBase 
	implements Reducer<Text, Text, Text, Text> 
	{
		private static int K = 0;
		
		public void configure(JobConf job) {
			K = Integer.parseInt(job.get("K"));
		}
		
		public void reduce(Text nodeid, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException 
		{
			Node node = new Node(nodeid.toString());
			
			List<String> threadmsgs = new ArrayList<String>();
			
			int sawnode = 0;
			
			while(iter.hasNext())
			{
				String msg = iter.next().toString();
				
				//System.err.println(nodeid.toString() + "\t" + msg);
				
				String [] vals = msg.split("\t");
				
				if (vals[0].equals(Node.NODEMSG))
				{
					node.parseNodeMsg(vals, 0);
					sawnode++;
				}
				else if (vals[0].equals(Node.THREADIBLEMSG))
				{
					String port = vals[1];
					threadmsgs.add(port);
				}
				else
				{
					throw new IOException("Unknown msgtype: " + msg);
				}
			}
			
			if (sawnode != 1)
			{
				throw new IOException("ERROR: Didn't see exactly 1 nodemsg (" + sawnode + ") for " + nodeid.toString());
			}
			
			for(String port : threadmsgs)
			{
				node.addThreadibleMsg(port);
			}
			
			output.collect(nodeid, new Text(node.toNodeMsg()));
		}
	}

	
	
	// Run Tool
	///////////////////////////////////////////////////////////////////////////	
	
	public RunningJob run(String inputPath, String outputPath) throws Exception
	{ 
		sLogger.info("Tool name: Threadible");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);
		
		JobConf conf = new JobConf(Stats.class);
		conf.setJobName("Threadible " + inputPath + " " + ContrailConfig.K);
		
		ContrailConfig.initializeConfiguration(conf);
			
		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(ThreadibleMapper.class);
		conf.setReducerClass(ThreadibleReducer.class);

		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return JobClient.runJob(conf);
	}
	

	// Parse Arguments and run
	///////////////////////////////////////////////////////////////////////////	

	public int run(String[] args) throws Exception 
	{
		String inputPath  = "/Users/mschatz/try/09-repeats.1.threads";
		String outputPath = "/users/mschatz/try/09-repeats.1.threadible";
		ContrailConfig.K = 21; 
		run(inputPath, outputPath);
		return 0;
	}


	// Main
	///////////////////////////////////////////////////////////////////////////	

	public static void main(String[] args) throws Exception 
	{
		int res = ToolRunner.run(new Configuration(), new Threadible(), args);
		System.exit(res);
	}
}
