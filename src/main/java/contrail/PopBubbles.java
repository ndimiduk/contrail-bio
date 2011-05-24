package contrail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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



public class PopBubbles extends Configured implements Tool 
{	
	private static final Logger sLogger = Logger.getLogger(PopBubbles.class);
	
	
	// PopBubblesMapper
	///////////////////////////////////////////////////////////////////////////
	
	private static class PopBubblesMapper extends MapReduceBase 
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

			List<String> bubbles = node.getBubbles();
			if (bubbles != null)
			{
				for(String bubble : bubbles)
				{
					String [] vals = bubble.split("\\|");
					String minor    = vals[0];
					String minord   = vals[1];
					String dead     = vals[2];
					String newd     = vals[3];
					String newid    = vals[4];
					String extracov = vals[5];

					output.collect(new Text(minor), 
							       new Text(Node.KILLLINKMSG + "\t" + minord + "\t" + dead + "\t" + newd + "\t" + newid));

					output.collect(new Text(dead), new Text(Node.KILLMSG));
					output.collect(new Text(newid), new Text(Node.EXTRACOV + "\t" + extracov));

					reporter.incrCounter("Contrail", "bubblespopped", 1);
				}

				node.clearBubbles();
			}

			output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
			reporter.incrCounter("Contrail", "nodes", 1);
		}
	}

	// PopBubblesReducer
	///////////////////////////////////////////////////////////////////////////

	private static class PopBubblesReducer extends MapReduceBase 
	implements Reducer<Text, Text, Text, Text> 
	{
		private static int K = 0;
		
		public void configure(JobConf job) {
			K = Integer.parseInt(job.get("K"));
		}
		
		public class ReplacementLink
		{
		    public String deaddir;
		    public String deadid;
		    public String newdir;
		    public String newid;
		    
		    public ReplacementLink(String[] vals, int offset) throws IOException
		    {
		    	if (!vals[offset].equals(Node.KILLLINKMSG))
		    	{
		    		throw new IOException("Unknown msg");
		    	}
		    	
		    	deaddir = vals[offset+1];
		    	deadid  = vals[offset+2];
		    	newdir  = vals[offset+3];
		    	newid   = vals[offset+4];
		    }
		}
		
		public void reduce(Text nodeid, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException 
		{
			Node node = new Node(nodeid.toString());

			int sawnode = 0;

			boolean killnode = false;
			float extracov = 0;
			List<ReplacementLink> links = new ArrayList<ReplacementLink>(); 

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
				else if (vals[0].equals(Node.KILLLINKMSG))
				{
					ReplacementLink link = new ReplacementLink(vals, 0);
					links.add(link);
				}
				else if (vals[0].equals(Node.KILLMSG))
				{
					killnode = true;
				}
				else if (vals[0].equals(Node.EXTRACOV))
				{
					extracov += Float.parseFloat(vals[1]);
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

			if (killnode)
			{
				reporter.incrCounter("Contrail", "bubblenodes_removed", 1);
				return;
			}

			if (extracov > 0)
			{
				int merlen = node.len() - K + 1;
				float support = node.cov() * merlen + extracov;
				node.setCoverage((float) support /  (float) merlen);
			}

			if (links.size() > 0)
			{
				for(ReplacementLink link : links)
				{
					node.removelink(link.deadid, link.deaddir);
					node.updateThreads(link.deaddir, link.deadid, link.newdir, link.newid);
					reporter.incrCounter("Contrail", "linksremoved", 1);
				}


				int threadsremoved = node.cleanThreads();
				reporter.incrCounter("Contrail", "threadsremoved", 1);
			}

			output.collect(nodeid, new Text(node.toNodeMsg()));
		}
	}

	
	
	
	// Run Tool
	///////////////////////////////////////////////////////////////////////////	
	
	public RunningJob run(String inputPath, String outputPath) throws Exception
	{ 
		sLogger.info("Tool name: PopBubbles");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);
		
		JobConf conf = new JobConf(Stats.class);
		conf.setJobName("PopBubbles " + inputPath + " " + ContrailConfig.K);
		
		ContrailConfig.initializeConfiguration(conf);
			
		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(PopBubblesMapper.class);
		conf.setReducerClass(PopBubblesReducer.class);

		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return JobClient.runJob(conf);
	}
	

	// Parse Arguments and run
	///////////////////////////////////////////////////////////////////////////	

	public int run(String[] args) throws Exception 
	{
		String inputPath  = "/Users/mschatz/try/05-popbubbles.1.f";
		String outputPath = "/users/mschatz/try/05-popbubbles.1";

		ContrailConfig.K = 21;
		
		run(inputPath, outputPath);
		return 0;
	}


	// Main
	///////////////////////////////////////////////////////////////////////////	

	public static void main(String[] args) throws Exception 
	{
		int res = ToolRunner.run(new Configuration(), new PopBubbles(), args);
		System.exit(res);
	}
}
