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


public class MateFinalize extends Configured implements Tool 
{	
	private static final Logger sLogger = Logger.getLogger(MateFinalize.class);
	
	public static boolean V = false;
	
	
	// MateFinalizeMapper
	///////////////////////////////////////////////////////////////////////////
	
	private static class MateFinalizeMapper extends MapReduceBase 
    implements Mapper<LongWritable, Text, Text, Text> 
	{
		public void map(LongWritable lineid, Text nodetxt,
                OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException 
        {
			Node node = new Node();
			node.fromNodeMsg(nodetxt.toString());
			
			V = node.getNodeId().equals("GMSRRSDLCJGRDHA");

			List<String> matethreads = node.getMateThreads();

			if (matethreads != null)
			{
				// Broadcast the thread messages
				String dir = null;
				String incoming = null;
				String cur = null;
				int idx = 0;
				String tandem = null;

				if (V)
				{
					System.err.println(">" + node.getNodeId());
					for(String tstr : matethreads)
					{
						System.err.println(tstr);
					}
				}

				for(String tstr : matethreads)
				{
					String [] vals = tstr.split(":");
					String td = vals[0];
					String vt = vals[1];
					String v  = vals[2];

					if (dir == null || !td.equals(dir))
					{
						// first hop out of me, clean up other bogus links in this direction
						dir = td;
						idx = 1;
						tandem = "P";

						int deg = node.degree(td);

						if (deg > 1)
						{
							// find bogus links in this direction
							for(String bdd : Node.dirs)
							{
								String bd = td + bdd;
								List<String> bdedges = node.getEdges(bd);

								if (bdedges != null)
								{
									for (String bv : bdedges)
									{
										if (!bd.equals(vt) || !bv.equals(v))
										{
											// bogus link to bv via bd
											// Don't delete the link right away, in case we need it for
											// other mates (shouldn't happen though)
											
											if (V)
											{
												System.err.println("Clean dead link from " + node.getNodeId() + " " + bd + ":" + bv + " < " + tstr);
											}
											
											// tell myself to kill the link
											output.collect(new Text(node.getNodeId()), 
														   new Text(Node.KILLLINKMSG + "\t" + bd + "\t" + bv));
											
											// tell my neighbors to kill the link
											output.collect(new Text(bv),
													       new Text(Node.KILLLINKMSG + "\t" + Node.flip_link(bd) + "\t" + node.getNodeId()));
										}
									}
								}
							}
						}

						incoming = Node.flip_link(vt) + ":" + node.getNodeId();
						cur = v;
					}
					else if (cur.equals(v))
					{
						// Went through a tandem
						incoming += "-" + Node.flip_link(vt);
						tandem = "T";
					}
					else
					{
						String outgoing = vt + ":" + v;
						String label = "b" + node.getNodeId() + "_" + idx + dir;

						output.collect(new Text(cur), new Text(Node.UPDATEMSG + "\t" + tandem + "-" + incoming + "-" + outgoing + "-" + label));
						
						if (V) { System.err.println("Emit Update from " + node.getNodeId() + " : " + cur + "\t" +
								                    Node.UPDATEMSG + "\t" + tandem + "-" + incoming + "-" + outgoing + "-" + label); 
						}

						idx++;
						incoming = Node.flip_link(vt) + ":" + cur;
						cur = v;
						tandem = "P";
					}
				}

				node.clearMateThreads();
			}

			node.clearThreads();
			node.clearBundles();
			node.clearThreadPath();

			output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
			reporter.incrCounter("Contrail", "nodes", 1);
         }
	}

	// MateFinalizeReducer
	///////////////////////////////////////////////////////////////////////////

	private static class MateFinalizeReducer extends MapReduceBase 
	implements Reducer<Text, Text, Text, Text> 
	{
		public class Edge
		{
			String et;
			String v;
			
			public Edge(String pet, String pv)
			{
				et = pet;
				v  = pv;
			}
			
			public String toString()
			{
				return et + ":" + v;
			}
		}
		
		public void reduce(Text nodeid, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException 
		{
			Node node = new Node(nodeid.toString());
			
			List<Edge> killlinks = new ArrayList<Edge>();
			List<String> updates = new ArrayList<String>();
			
			int sawnode = 0;
			
			V = node.getNodeId().equals("GMSRRSDLCJGRDHA");
			
			while(iter.hasNext())
			{
				String msg = iter.next().toString();
				
				if (V) { System.err.println("MateFinalize.reduce: " + nodeid.toString() + "\t" + msg); }
				
				String [] vals = msg.split("\t");
				
				if (vals[0].equals(Node.NODEMSG))
				{
					node.parseNodeMsg(vals, 0);
					sawnode++;
				}
				else if (vals[0].equals(Node.UPDATEMSG))
				{
					updates.add(vals[1]);
				}
				else if (vals[0].equals(Node.KILLLINKMSG))
				{
					killlinks.add(new Edge(vals[1], vals[2]));
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
			
			Set<String> killports = new HashSet<String>();

			if (killlinks.size() > 0)
			{
				for(Edge kill : killlinks)
				{
					if (V) { System.err.println("Removing " + node.getNodeId() + " " + kill.toString()); }
					output.collect(new Text(node.getNodeId()), new Text(Node.KILLLINKMSG + "\t" + kill.et + "\t" + kill.v));
					killports.add(kill.et + ":" + kill.v);
				}
			}

			int fdegree = node.degree("f");
			int rdegree = node.degree("r");

			if ((fdegree > 1) || (rdegree > 1))
			{
				if (updates.size() > 0)
				{
					reporter.incrCounter("Contrail", "updates", 1);

					for (String thread : updates)
					{
						if (V) { System.err.println("Processing: " + node.getNodeId() + " " + thread); }
						String [] steps = thread.split("-");

						String fport = steps[1];
						String rport = steps[steps.length-2];

						//String [] fvals = fport.split(":");
						//String ft = fvals[0];
						//String fn = fvals[1];

						//String [] rvals = rport.split(":");
						//String rt = rvals[0];
						//String rn = rvals[1];
						
						if (killports.contains(fport) || killports.contains(rport))
						{
							if (V) { System.err.println(" Don't use this thread, link was killed"); }
						}
						else
						{
							node.addThreadPath(thread);
						}
					}
				}
			}
			
			output.collect(nodeid, new Text(node.toNodeMsg()));
		}
	}

	
	
	
	// Run Tool
	///////////////////////////////////////////////////////////////////////////	
	
	public RunningJob run(String inputPath, String outputPath) throws Exception
	{ 
		sLogger.info("Tool name: MateFinalize");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);
		
		JobConf conf = new JobConf(Stats.class);
		conf.setJobName("MateFinalize " + inputPath);
		
		ContrailConfig.initializeConfiguration(conf);
			
		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(MateFinalizeMapper.class);
		conf.setReducerClass(MateFinalizeReducer.class);

		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return JobClient.runJob(conf);
	}
	

	// Parse Arguments and run
	///////////////////////////////////////////////////////////////////////////	

	public int run(String[] args) throws Exception 
	{
		String inputPath  = "/users/mschatz/contrail/Ec200k/11-scaffold.2.matepath";
		String outputPath = "/users/mschatz/contrail/Ec200k/11-scaffold.2.final";
		
		run(inputPath, outputPath);
		return 0;
	}


	// Main
	///////////////////////////////////////////////////////////////////////////	

	public static void main(String[] args) throws Exception 
	{
		int res = ToolRunner.run(new Configuration(), new MateFinalize(), args);
		System.exit(res);
	}
}
