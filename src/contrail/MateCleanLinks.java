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


public class MateCleanLinks extends Configured implements Tool 
{	
	private static final Logger sLogger = Logger.getLogger(MateCleanLinks.class);
	
	public static boolean V = false;
	
	// MateCleanLinksMapper
	///////////////////////////////////////////////////////////////////////////
	
	private static class MateCleanLinksMapper extends MapReduceBase 
    implements Mapper<LongWritable, Text, Text, Text> 
	{
		public void map(LongWritable lineid, Text nodetxt,
                OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException 
        {
			// Repeat the node and killlink messages
			String msg = nodetxt.toString();
			String [] vals = msg.split("\t");
			
			output.collect(new Text(vals[0]),
					       new Text(Node.joinstr("\t", vals, 1)));
			
			reporter.incrCounter("Contrail", "msgs", 1);
        }
	}

	// MateCleanLinksReducer
	///////////////////////////////////////////////////////////////////////////

	private static class MateCleanLinksReducer extends MapReduceBase 
	implements Reducer<Text, Text, Text, Text> 
	{
		public class Edge
		{
			String et;
			String v;
			
			public Edge (String [] vals, int offset)
			{
				et = vals[offset];
				v  = vals[offset+1];
			}
			
			public String toString()
			{
				return et + ":" + v;
			}
			
			public int hashCode()
			{
				return toString().hashCode();
			}
			
			public boolean equals(Object o)
			{
				Edge e = (Edge) o;
				return toString().equals(e.toString());
			}
		}
		
		public void reduce(Text nodeid, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException 
		{
			Node node = new Node(nodeid.toString());
			
			V = node.getNodeId().equals("GMSRRSDLCJGRDHA");
			
			Set<Edge> deadedges = new HashSet<Edge>();
			
			int sawnode = 0;
			
			while(iter.hasNext())
			{
				String msg = iter.next().toString();
				
				if (V) { System.err.println(nodeid.toString() + "\t" + msg); }
				
				String [] vals = msg.split("\t");
				
				if (vals[0].equals(Node.NODEMSG))
				{
					node.parseNodeMsg(vals, 0);
					sawnode++;
				}
				else if (vals[0].equals(Node.KILLLINKMSG))
				{
					Edge e = new Edge(vals, 1);
					if (!deadedges.add(e))
					{
						System.err.println("can't remove same link twice: " + e.toString());
					}
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
			
			
			if (!deadedges.isEmpty())
			{
				Iterator<Edge> de = deadedges.iterator();
				long removed_edges = 0;
				
				while (de.hasNext())
				{
					Edge e = de.next();
					
					if (V) { System.err.println("Removing " + node.getNodeId() + " " + e.toString()); }
					
					node.removelink(e.v, e.et);
					removed_edges++;
				}
				
				reporter.incrCounter("Contrail", "removed_edges", removed_edges);
			}
			
			output.collect(nodeid, new Text(node.toNodeMsg()));
		}
	}

	
	
	
	// Run Tool
	///////////////////////////////////////////////////////////////////////////	
	
	public RunningJob run(String inputPath, String outputPath) throws Exception
	{ 
		sLogger.info("Tool name: MateCleanLinks");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);
		
		JobConf conf = new JobConf(Stats.class);
		conf.setJobName("MateCleanLinks " + inputPath);
		
		ContrailConfig.initializeConfiguration(conf);
		
		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(MateCleanLinksMapper.class);
		conf.setReducerClass(MateCleanLinksReducer.class);

		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return JobClient.runJob(conf);
	}
	

	// Parse Arguments and run
	///////////////////////////////////////////////////////////////////////////	

	public int run(String[] args) throws Exception 
	{
		String inputPath  = "/Users/mschatz/contrail/Ec500k.cor.21/11-scaffold.1.final";
		String outputPath = "/users/mschatz/cleanout";
		
		run(inputPath, outputPath);
		return 0;
	}


	// Main
	///////////////////////////////////////////////////////////////////////////	

	public static void main(String[] args) throws Exception 
	{
		int res = ToolRunner.run(new Configuration(), new MateCleanLinks(), args);
		System.exit(res);
	}
}
