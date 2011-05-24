package contrail;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
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


public class ThreadRepeats extends Configured implements Tool 
{	
	private static final Logger sLogger = Logger.getLogger(ThreadRepeats.class);


	// ThreadRepeatsMapper
	///////////////////////////////////////////////////////////////////////////

	private static class ThreadRepeatsMapper extends MapReduceBase 
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

			List<String> threads = node.getThreads();

			if (threads != null)
			{
				Map<String, StringBuffer> threadidx = new HashMap<String, StringBuffer>();

				// Index the edges
				for(String adj: Node.edgetypes)
				{
					List<String> edges = node.getEdges(adj);

					if (edges != null)
					{
						for(String edge : edges)
						{
							String link = adj + ":" + edge;
							threadidx.put(link, new StringBuffer());
						}
					}
				}

				// Index the threading reads
				for(String thread : threads)
				{
					String [] vals = thread.split(":");
					String t    = vals[0];
					String link = vals[1];
					String read = vals[2];

					String key = t + ":" + link;

					if (threadidx.containsKey(key))
					{
						StringBuffer b = threadidx.get(key);
						b.append("\t").append(read);
					}
				}

				for(String e: threadidx.keySet())
				{
					StringBuffer b = threadidx.get(e);
					if (b.length() > 0)
					{
						String [] vals = e.split(":");
						String tdir = vals[0];
						String link = vals[1];

						String f = Node.flip_link(tdir);

						output.collect(new Text(link), 
								new Text(Node.UPDATEMSG + "\t" + f + "\t" + node.getNodeId() + b.toString()));
					}
				}
			}
		}
	}



	// ThreadRepeatsReducer
	///////////////////////////////////////////////////////////////////////////

	private static class ThreadRepeatsReducer extends MapReduceBase 
	implements Reducer<Text, Text, Text, Text> 
	{
		private static int K = 0;
		private static int MIN_THREAD_WEIGHT = 0;
		public static boolean V = false;

		public void configure(JobConf job) {
			K = Integer.parseInt(job.get("K"));
			MIN_THREAD_WEIGHT = Integer.parseInt(job.get("MIN_THREAD_WEIGHT"));
		}

		public void reduce(Text nodeid, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException 
		{
			Node node = new Node(nodeid.toString());

			Map<String, Set<String>> newthreads = new HashMap<String, Set<String>>();

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
				else if (vals[0].equals(Node.UPDATEMSG))
				{
					String dir = vals[1];
					String nid = vals[2];

					String link = dir + ":" + nid;

					Set<String> reads = newthreads.get(link);
					if (reads == null)
					{
						reads = new HashSet<String>();
						newthreads.put(link, reads);
					}

					for(int i = 3; i < vals.length; i++)
					{
						reads.add(vals[i]);
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

			List<String> oldthreads = node.getThreads();

			// Add in the old threads
			if ((oldthreads != null) && (oldthreads.size() > 0))
			{
				// Index the threading reads
				for(String thread : oldthreads)
				{
					String [] vals = thread.split(":");
					String dir  = vals[0];
					String nid = vals[1];
					String read = vals[2];

					String link = dir + ":" + nid;

					Set<String> reads = newthreads.get(link);
					if (reads == null)
					{
						reads = new HashSet<String>();
						newthreads.put(link, reads);
					}

					reads.add(read);
				}

				node.clearThreads();
			}

			// Save away the current threads
			boolean tandem = false;

			for(String adj: Node.edgetypes)
			{
				List<String> edgelist = node.getEdges(adj);

				if (edgelist != null)
				{
					for(String edge : edgelist)
					{
						String link = adj + ":" + edge;

						if (edge.equals(node.getNodeId()))
						{
							tandem = true;
						}

						if (newthreads.containsKey(link))
						{
							Set<String> reads = newthreads.get(link);
							for (String read : reads)
							{
								node.addThread(adj, edge, read);
							}
						}
					}
				}
			}

			int fd = node.degree("f");
			int rd = node.degree("r");

			if ((fd <= 1) && (rd <= 1))
			{
				// This is NOT a branching node, nothing to do
				node.clearThreads();
			}
			else if (tandem)
			{
				// Don't attempt to thread through tandems
				reporter.incrCounter("Contrail", "tandem", 1);
			}
			else if ((fd == 0) || (rd == 0))
			{
				// Deadend node, could be a palidrome though
				reporter.incrCounter("Contrail", "deadend", 1);
				//node.setThreadPath("D");
			}
			else if (false) // && (($fd == 1) || ($rd == 1)))
			{
				// Half decision node, split

				// Index the threading reads
				Map<String, List<String>> reads = new HashMap<String, List<String>>();

				for(String thread : node.getThreads())
				{
					if (V) { System.err.println(node.getNodeId() + " " + thread); }
					String [] vals = thread.split(":");
					String t    = vals[0];
					String link = vals[1];
					String read = vals[2];

					String key = t + ":" + link;

					List<String> tlist;
					if (reads.containsKey(key))
					{
						tlist = reads.get(key);
					}
					else
					{
						tlist = new ArrayList<String>();
						reads.put(key, tlist);
					}

					tlist.add(read);
				}

				boolean valid = true;

				for (String read : reads.keySet())
				{
					List<String> ports = reads.get(read);

					if ((ports.size() > 2) ||
							((ports.size() == 2) && (ports.get(0).charAt(0) == ports.get(1).charAt(0))))
					{
						valid = false;
						break;
					}
				}

				if (!valid)
				{
					reporter.incrCounter("Contrail", "invalidhalf", 1);
					System.err.println("Invalid Half for " + node.getNodeId());
				}

				reporter.incrCounter("Contrail", "halfdecision", 1);
				reporter.incrCounter("Contrail", "threadible", 1);
				node.setThreadPath("H");
			}
			else
			{
				// I'm an X-node. See if there are read threads for all pairs

				if ((fd < 2) || (rd < 2)) { reporter.incrCounter("Contrail", "halfdecision", 1); }

				// Index the threading reads
				Map<String, List<String>> reads = new HashMap<String, List<String>>();
				
				if (V) { System.err.println("threads"); }

				for(String thread : node.getThreads())
				{
					if (V) { System.err.println(node.getNodeId() + " " + thread); }
					String [] vals = thread.split(":");
					String t    = vals[0];
					String link = vals[1];
					String read = vals[2];

					String port = t + ":" + link;

					List<String> tlist;
					if (reads.containsKey(read))
					{
						tlist = reads.get(read);
					}
					else
					{
						tlist = new ArrayList<String>();
						reads.put(read, tlist);
					}

					tlist.add(port);
				}

				// Index the pairs
				Map<String, Integer> portreads = new HashMap<String, Integer>();
				
				if (V) { System.err.println("ports"); }

				for(String read : reads.keySet())
				{
					// If there are more than 2, then there was a name collision
					// Also make sure we have a f and r link
					List<String> ports = reads.get(read);
					
					if (V) { System.err.println(read + " " + Node.joinstr(" ", ports)); }

					if (ports != null && ports.size() == 2)
					{
						String fport = ports.get(0);
						String rport = ports.get(1);

						// make sure it is an f-r thread
						if (fport.charAt(0) != rport.charAt(0))
						{
							String portname;

							if (rport.charAt(0) == 'f')
							{
								portname = rport + "-" + fport;
							}
							else
							{
								portname = fport + "-" + rport;
							}
							
							if (V) { System.err.println("port: " + portname); }

							if (portreads.containsKey(portname)) 
							{ 
								Integer i = portreads.get(portname);
								i++;
							}
							else
							{
								portreads.put(portname, new Integer(1));
							}
						}
					}
				}
				
				if (V) { System.err.println("Links"); }

				
				// See if there is a thread from every edge to some other edge
				boolean haveall = true;

				OUTER:
					for(String xa : Node.dirs)
					{
						for(String ya : Node.dirs)
						{
							String ta = xa + ya;

							List<String> edgesa = node.getEdges(ta);

							if (edgesa != null)
							{
								for(String edgea : edgesa)
								{
									if (V) { System.err.println("Checking: " + edgea); }
									int validports = 0;

									String xb = Node.flip_dir(xa);

									for (String yb : Node.dirs)
									{
										String tb = xb + yb;
										List<String> edgesb = node.getEdges(tb);

										if (edgesb != null)
										{
											for (String edgeb : edgesb)
											{
												String portname;

												if (xa.charAt(0) == 'f')
												{
													portname = ta + ":" + edgea + "-" + tb + ":" + edgeb; 
												}
												else
												{
													portname = tb + ":" + edgeb + "-" + ta + ":" + edgea;
												}

												int weight = 0;

												if (portreads.containsKey(portname))
												{
													weight = portreads.get(portname).intValue();
												}

												if (V) { System.err.println(portname + " " + weight); }

												if (weight >= MIN_THREAD_WEIGHT)
												{
													validports++;
												}
											}
										}
									}
									
									if (V) { System.err.println("validports: " + validports); }

									if (validports == 0)
									{
										haveall = false;
										break OUTER;
									}
								}
							}
						}
					}

				if (haveall)
				{
					reporter.incrCounter("Contrail", "xcut", 1);
					reporter.incrCounter("Contrail", "threadible", 1);
					node.setThreadPath("X");
				}
			}

			output.collect(nodeid, new Text(node.toNodeMsg()));
		}
	}



	// Run Tool
	///////////////////////////////////////////////////////////////////////////	

	public RunningJob run(String inputPath, String outputPath) throws Exception
	{ 
		sLogger.info("Tool name: ThreadRepeats");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(Stats.class);
		conf.setJobName("ThreadRepeats " + inputPath + " " + ContrailConfig.K);
		
		ContrailConfig.initializeConfiguration(conf);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(ThreadRepeatsMapper.class);
		conf.setReducerClass(ThreadRepeatsReducer.class);

		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return JobClient.runJob(conf);
	}


	// Parse Arguments and run
	///////////////////////////////////////////////////////////////////////////	

	public int run(String[] args) throws Exception 
	{
		String inputPath  = "/Users/mschatz/try/08-lowcovcmp";
		String outputPath = "/users/mschatz/try/09-repeats.1.threads";
		
		ContrailConfig.K = 21;
		ContrailConfig.MIN_THREAD_WEIGHT = 1;
		
		run(inputPath, outputPath);
		return 0;
	}


	// Main
	///////////////////////////////////////////////////////////////////////////	

	public static void main(String[] args) throws Exception 
	{
		int res = ToolRunner.run(new Configuration(), new ThreadRepeats(), args);
		System.exit(res);
	}
}
