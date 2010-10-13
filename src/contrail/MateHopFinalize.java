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


// Find the closest linked neighbor in both directions

public class MateHopFinalize extends Configured implements Tool 
{	
	private static final Logger sLogger = Logger.getLogger(MateHopFinalize.class);
	public static boolean V = false;


	// MateHopFinalizeMapper
	///////////////////////////////////////////////////////////////////////////

	private static class MateHopFinalizeMapper extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, Text> 
	{
		public void map(LongWritable lineid, Text nodetxt,
				OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException 
		{
			Node node = new Node();
			node.fromNodeMsg(nodetxt.toString());
			
			// Clean out aborted threads
			List<String> threads = node.getMateThreads();
			if (threads != null)
			{
				reporter.incrCounter("Contrail", "aborted_threads", threads.size());
				reporter.incrCounter("Contrail", "aborted_threads_nodes", 1);
				node.clearMateThreads();
			}

			output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
			reporter.incrCounter("Contrail", "nodes", 1);
		}
	}

	// MateHopFinalizeReducer
	///////////////////////////////////////////////////////////////////////////

	private static class MateHopFinalizeReducer extends MapReduceBase 
	implements Reducer<Text, Text, Text, Text> 
	{
		public void reduce(Text nodeid, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException 
		{
			Node node = new Node(nodeid.toString());
			
			V = node.getNodeId().equals("GMSRRSDLCJGRDHA");

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
				else
				{
					throw new IOException("Unknown msgtype: " + msg);
				}
			}

			if (sawnode != 1)
			{
				throw new IOException("ERROR: Didn't see exactly 1 nodemsg (" + sawnode + ") for " + nodeid.toString());
			}

			List<String> bundles = node.getBundles();

			if (bundles != null)
			{
				// organize paths to me from starting nodes K. There may be multiple paths from K, so store them in a list
				Map<String, List<String>> paths = new HashMap<String,List<String>>();

				for (String b : bundles)
				{
					if (b.charAt(0) == '#')
					{
						b = b.substring(1);
						String [] path = b.split(":");

						String startnode = path[0];
						String endnode = path[path.length - 1];

						if (endnode.equals(node.getNodeId()))
						{
							// Found a complete path to me
							if (paths.containsKey(startnode))
							{
								List<String> plist = paths.get(startnode);
								plist.add(b);
							}
							else
							{
								List<String> plist = new ArrayList<String>();
								plist.add(b);
								paths.put(startnode, plist);
							}
						}
						else
						{
							// bundlemsg, skip
						}
					}
				}

				// Find the nearest path in the f & r directions
				String [] dirpaths = new String[2]; dirpaths[0] = null; dirpaths[1] = null;
				String [] firsthops = new String[2]; firsthops[0] = null; firsthops[1] = null;
				boolean [] firsthopunique = {true, true};

				for(String startnode : paths.keySet())
				{
					List<String> plist = paths.get(startnode);

					if (plist.size() > 1)
					{
						// path is ambiguous
						
						if (V)
						{
							System.err.println("Found ambigous path");
							for (int i = 0; i < plist.size(); i++)
							{
								System.err.println(i + ": " + plist.get(i));
							}
						}
						
						reporter.incrCounter("Contrail", "total_ambiguous", 1);
						continue;
					}

					// found a unique consistent path from startnode to me
					
					String pathstr = plist.get(0);
					
					String [] hops = pathstr.split(":");

					int curidx = hops.length-1;
					//String curnode = hops[curidx];
					String curedge = Node.flip_link(hops[curidx-1]);
					String firsthop = hops[curidx-2];
					char ut = curedge.charAt(0);
					
					if (V) { System.err.println("Found unique path " + node.getNodeId() + " " + ut + " " + startnode + " : " + pathstr); }
					
					if (ut == 'f')
					{
						if (firsthops[0] == null) { firsthops[0] = firsthop; }
						else if (!firsthop.equals(firsthops[0])) 
						{
							if (V) { System.err.println("First hop is not unique: " + ut); }
							firsthopunique[0] = false; 
						}
						
						if ((dirpaths[0] == null) || (pathstr.length() < dirpaths[0].length())) { dirpaths[0] = pathstr; } 
					}
					else
					{
						if (firsthops[1] == null) { firsthops[1] = firsthop; }
						else if (!firsthop.equals(firsthops[1])) 
						{
							if (V) { System.err.println("First hop is not unique: " + ut); }
							firsthopunique[1] = false; 
						}
						
						if ((dirpaths[1] == null) || (pathstr.length() < dirpaths[1].length())) { dirpaths[1] = pathstr; }
					}
				}
				
				//if (!firsthopunique[0]) { dirpaths[0] = null; }
				//if (!firsthopunique[1]) { dirpaths[1] = null; }
				
				for (String pathstr : dirpaths)
				{
					if (pathstr != null)
					{
						if (V) { System.err.println("Keeping: " + pathstr); }

						String [] hops = pathstr.split(":");

						int curidx = hops.length-1;
						String curnode = hops[curidx];
						String curedge = Node.flip_link(hops[curidx-1]);
						char ut = curedge.charAt(0);

						curidx--;

						while (curidx > 0)
						{
							curedge = Node.flip_link(hops[curidx]);
							curnode = hops[curidx-1];

							node.addMateThread(ut + ":" + curedge + ":" + curnode);

							curidx -= 2;

							reporter.incrCounter("Contrail", "resolved_edges", 1);
						}

						reporter.incrCounter("Contrail", "resolved_bundles", 1);
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
		sLogger.info("Tool name: MateHopFinalize");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(Stats.class);
		conf.setJobName("MateHopFinalize " + inputPath);
		
		ContrailConfig.initializeConfiguration(conf);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(MateHopFinalizeMapper.class);
		conf.setReducerClass(MateHopFinalizeReducer.class);

		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return JobClient.runJob(conf);
	}


	// Parse Arguments and run
	///////////////////////////////////////////////////////////////////////////	

	public int run(String[] args) throws Exception 
	{
		String inputPath  = "/users/mschatz/contrail/Ec100k/11-scaffold.3.search9";
		String outputPath = "/users/mschatz/contrail/Ec100k/11-scaffold.3.matepath";

		run(inputPath, outputPath);
		return 0;
	}


	// Main
	///////////////////////////////////////////////////////////////////////////	

	public static void main(String[] args) throws Exception 
	{
		int res = ToolRunner.run(new Configuration(), new MateHopFinalize(), args);
		System.exit(res);
	}
}
