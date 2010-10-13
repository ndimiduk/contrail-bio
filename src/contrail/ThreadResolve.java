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


public class ThreadResolve extends Configured implements Tool 
{	
	private static final Logger sLogger = Logger.getLogger(ThreadResolve.class);

	public static boolean V = false;
	

	// ThreadResolveMapper
	///////////////////////////////////////////////////////////////////////////

	private static class ThreadResolveMapper extends MapReduceBase 
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
			
			//V = node.getNodeId().equals("GMSRRSDLCJGRDHA");

			reporter.incrCounter("Contrail", "nodes", 1);

			boolean print_node = true;

			List<String> threadpaths = node.getThreadPath();

			if (threadpaths != null)
			{
				reporter.incrCounter("Contrail", "allneedsplit", 1);

				if (V) { System.err.println("Selecting master for " + node.getNodeId() + " : " + Node.joinstr(" ", threadpaths)); }

				String masterid = node.getNodeId();

				List<String> threadiblemsgs = node.getThreadibleMsgs();

				if ((threadiblemsgs != null) && (threadiblemsgs.size() > 0))
				{
					if (V) { System.err.println(" threaded neighbors: " + Node.joinstr(" ", threadiblemsgs)); }

					for (String port : threadiblemsgs)
					{
						String [] vals = port.split(":");
						String t = vals[0];
						String v = vals[1];

						if (v.compareTo(masterid) < 0)
						{
							masterid = v;
						}
					}
				}

				if (!masterid.equals(node.getNodeId()))
				{
					if (V) { System.err.println("Skipping " + node.getNodeId() + " waiting for " + masterid); }
					reporter.incrCounter("Contrail", "needsplit", 1);
				}
				else
				{
					int totalthreadedbp = 0;

					// I'm the master of my local neighborhood
					if (V)  { System.err.println("== Resolving " + node.getNodeId());}
					reporter.incrCounter("Contrail", "resolved", 1);

					print_node = false;
					boolean tandem = false;
					
					for(String et : Node.edgetypes)
					{
						List<String> edges = node.getEdges(et);
						
						if (edges != null)
						{
							if (V) { System.err.print(" " + et); }

							for(String v : edges)
							{
								if (V) { System.err.print(" " + v); }
								
								if (v.equals(node.getNodeId()))
								{
									tandem = true;
								}
							}
							
							if (V) { System.err.println(); }
						}
					}

					int fd = node.degree("f");
					int rd = node.degree("r");

					if ((fd <= 1) && (rd <= 1))
					{
						// I'm not a branching node
						System.err.println("WARNING: supposed to split a non-branching node " + node.getNodeId());

						node.clearThreadPath();

						output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));

						for (String et : Node.edgetypes)
						{
							List<String> edges = node.getEdges(et);
							if (edges != null)
							{
								String ret = Node.flip_link(et);
								for(String v : edges)
								{
									output.collect(new Text(v), 
											new Text(Node.RESOLVETHREADMSG + "\t" + ret + "\t" + node.getNodeId() + "\t" + node.getNodeId()));
								}
							}
						}

						return;
					}

					if (tandem)
					{
						System.err.println("WARNING: splitting a tandem " + node.getNodeId());
					}

					Map<String, List<String>> pairs = new HashMap<String,List<String>>();

					if ((threadpaths.size() > 1) ||
							(threadpaths.get(0).charAt(0) == 'P') ||
							(threadpaths.get(0).charAt(0) == 'T'))
					{
						if (V) { System.err.println("Threaded mates"); }
						
						// Mates were threaded through this node
						for(String path : threadpaths)
						{
							String [] fields = path.split("-");

							String t = fields[0];
							String msg = fields[fields.length-1];

							if (t.charAt(0) == 'T' || t.charAt(0) == 'P')
							{
								// fields 1 through fields.length-2 define the mate path
								StringBuilder sb = new StringBuilder();

								if (fields[fields.length-2].compareTo(fields[1]) < 0)
								{
									sb.append(fields[fields.length-2]);

									int offset = fields.length-3;
									while (offset > 1)
									{
										sb.append('-');
										sb.append(Node.flip_link(fields[offset]));
										offset--;
									}
									sb.append('-');
									sb.append(fields[1]);
								}
								else
								{
									sb.append(fields[1]);
									for (int offset = 2; offset < fields.length-1; offset++)
									{
										sb.append('-');
										sb.append(fields[offset]);
									}
								}

								String key = sb.toString();
								String val = "*" + t + msg;

								if (V) { System.err.println(key + " " + val); }

								if (pairs.containsKey(key))
								{
									pairs.get(key).add(val);
								}
								else
								{
									List<String> plist = new ArrayList<String>();
									plist.add(val);
									pairs.put(key, plist);
								}
							}
							else
							{
								throw new IOException("Unknown path msg type: " + path);
							}
						}
					}
					else
					{
						if (V) { System.err.println("X-cut thread"); }
						// X-cut or half decision node

						if ((fd <= 1) || (rd <= 1))
						{
							if (V) { System.err.println("Spliting half-decision node"); }

							// I'm a half-decision, do full split
							String unique = null;

							if      (fd == 1) { unique = "f"; }
							else if (rd == 1) { unique = "r"; }

							if (unique != null)
							{
								TailInfo unitail = node.gettail(unique);
								String uniquelink = unique + unitail.dir;

								String nonunique = Node.flip_dir(unique);

								for (String x : Node.dirs)
								{
									String t = nonunique + x;

									List<String> edges = node.getEdges(t);
									if (edges != null)
									{
										for(String v : edges)
										{
											String l;

											if (uniquelink.compareTo(t) <= 0)
											{
												// f-r
												l = uniquelink + ":" + unitail.id + "-" + t + ":" + v;
											}
											else
											{
												// r-f
												l = t + ":" + v + "-" + uniquelink + ":" + unitail.id;
											}

											if (pairs.containsKey(l))
											{
												pairs.get(l).add("*half");
											}
											else
											{
												List<String> plist = new ArrayList<String>();
												plist.add("*half");
												pairs.put(l, plist);
											}
										}
									}
									else
									{
										// I'm a deadend
									}
								}
							}
						}

						// If I'm an X-cut or a half decision, keep track of the spanning reads
						Map<String, List<String>> reads = new HashMap<String, List<String>>();

						// Index the threading reads
						List<String> threads = node.getThreads();

						for (String thread : threads)
						{
							String [] vals = thread.split(":");

							String t    = vals[0];
							String link = vals[1];
							String read = vals[2];

							String port = t + ":" + link;

							if (reads.containsKey(read))
							{
								reads.get(read).add(port);
							}
							else
							{
								List<String> rlist = new ArrayList<String>();
								rlist.add(port);
								reads.put(read, rlist);
							}
						}

						// Index the pairs
						for (String read : reads.keySet())
						{
							List<String> rlist = reads.get(read);

							if (rlist.size() == 2)
							{
								String l = null;

								if ((rlist.get(0).charAt(0) == 'f') && (rlist.get(1).charAt(0) == 'r'))
								{
									l = rlist.get(0) + "-" + rlist.get(1);
								}
								else if ((rlist.get(0).charAt(0) == 'r') && (rlist.get(1).charAt(0) == 'f'))
								{
									l = rlist.get(1) + "-" + rlist.get(0);
								}

								if (l != null)
								{
									if (pairs.containsKey(l))
									{
										pairs.get(l).add(read);
									}
									else
									{
										List<String> plist = new ArrayList<String>();
										plist.add(read);
										pairs.put(l, plist);
									}
								}
							}
						}
					}

					Set<String> portstatus = new HashSet<String>();

					String str = node.str();

					int threadedbp = str.length() - K + 1;
					reporter.incrCounter("Contrail", "uniquethreadedbp", threadedbp);

					int copies = pairs.keySet().size(); 

					// Now unzip for reads that span the node
					int copy = 0;

					for (String pt : pairs.keySet())
					{
						List<String> rlist = pairs.get(pt);
						if (V) { System.err.println("Resolving " + pt + " " + Node.joinstr(" ", rlist)); }

						if (rlist.get(0).startsWith("*T"))
						{
							// Tandem

							String [] path = pt.split("-");

							//   0  -  1  -  2  -      3     -  4
							//  Foo - me0 - me1 - (implicit) - Bar

							//  selfcopies = 5 - 2
							//  first copy = 0
							//  mid copy   = 1
							//  last  copy = 2

							int selfcopies = path.length - 2 + 1;

							if (V) {  System.err.println("Resolving tandem " + node.getNodeId() + 
									                     " into " + selfcopies + 
									                     " copies " + pt + 
									                     " : " + Node.joinstr(",", rlist)); }

							for (int i = 0; i < selfcopies; i++)
							{
								copy++;
								totalthreadedbp += threadedbp;

								int p = copy - 1;
								int n = copy + 1;

								String previd    = node.getNodeId() + "_" + p; 
								String nextid    = node.getNodeId() + "_" + n; 
								String newnodeid = node.getNodeId() + "_" + copy;

								String aport;
								String bport;

								if (i == 0)
								{
									aport = path[0];
									portstatus.add(aport);
								}
								else
								{
									String adir = Node.flip_link(path[i]);
									aport = adir + ":" + previd;
									portstatus.add(adir + ":" + node.getNodeId());
								}

								if (i == selfcopies - 1)
								{
									bport = path[i+1];
									portstatus.add(bport);
								}
								else
								{
									String bdir = path[i+1];
									bport = bdir + ":" + nextid;
									portstatus.add(bdir + ":" + node.getNodeId());
								}

								String [] avals = aport.split(":");
								String at    = avals[0];
								String alink = avals[1];

								String [] bvals = bport.split(":");
								String bt    = bvals[0];
								String blink = bvals[1];

								Node newnode = new Node(newnodeid);
								newnode.addEdge(at, alink);
								newnode.addEdge(bt, blink);
								newnode.setstr(str);
								newnode.setCoverage(1.0f);

								output.collect(new Text(newnode.getNodeId()), new Text(newnode.toNodeMsg()));

								if (V) { System.err.println("  " + alink + " " + at + " " + newnodeid + " " + bt + " " + blink); }

								if (i == 0)
								{
									at = Node.flip_link(at);
									output.collect(new Text(alink), 
											new Text(Node.RESOLVETHREADMSG + "\t" + at + "\t" + node.getNodeId() + "\t" + newnodeid));
								}

								if (i == selfcopies - 1)
								{
									bt = Node.flip_link(bt);
									output.collect(new Text(blink), 
											new Text(Node.RESOLVETHREADMSG + "\t" + bt + "\t" + node.getNodeId() + "\t" + newnodeid));
								}
							}
						}
						else
						{
							// simple non tandem

							// Foo - (implicit copy) - Bar

							copy++;
							totalthreadedbp += threadedbp;

							String newnodeid = node.getNodeId() + "_" + copy;

							if (V) { System.err.println(newnodeid + " " + pt + ":" + Node.joinstr(",", rlist)); }

							String [] ports = pt.split("-");
							String aport = ports[0];
							String bport = ports[1];

							String [] avals = aport.split(":");
							String at    = avals[0];
							String alink = avals[1];

							String [] bvals = bport.split(":");
							String bt    = bvals[0];
							String blink = bvals[1];

							Node newnode = new Node(newnodeid);
							newnode.addEdge(at, alink);
							newnode.addEdge(bt, blink);
							newnode.setstr(str);

							newnode.setCoverage(node.cov() / copies);

							for(String tread : pairs.get(pt))
							{
								if (tread.charAt(0) != '*')
								{
									newnode.addThread(at, alink, tread);
									newnode.addThread(bt, blink, tread);
								}
							}

							output.collect(new Text(newnode.getNodeId()), new Text(newnode.toNodeMsg()));

							at = Node.flip_link(at);
							bt = Node.flip_link(bt);

							output.collect(new Text(alink), 
									new Text(Node.RESOLVETHREADMSG + "\t" + at + "\t" + node.getNodeId() + "\t" + newnodeid));

							output.collect(new Text(blink), 
									new Text(Node.RESOLVETHREADMSG + "\t" + bt + "\t" + node.getNodeId() + "\t" + newnodeid));

							portstatus.add(aport);
							portstatus.add(bport);
						}
					}

					// Check for dangling (non-spanned) ports
					// copy this node, but separate from graph

					for (String t : Node.edgetypes)
					{
						List<String> edges = node.getEdges(t);
						if (edges != null)
						{
							for (String nn : edges)
							{
								if (!portstatus.contains(t + ":" + nn))
								{
									if (nn.equals(node.getNodeId()))
									{
										System.err.println(" WARNING: tandem repeat is not fully resolved");
										continue;
									}

									copy++;
									totalthreadedbp += threadedbp;

									String newnodeid = node.getNodeId() + "_" + copy;

									if (V) { System.err.println("  " + newnodeid + " half-split - " + t + ":" + nn); }

									Node newnode = new Node(newnodeid);
									newnode.addEdge(t, nn);
									newnode.setstr(str);
									newnode.setCoverage(1.0f);

									output.collect(new Text(newnode.getNodeId()), new Text(newnode.toNodeMsg()));

									String rt = Node.flip_link(t);

									output.collect(new Text(nn), 
											new Text(Node.RESOLVETHREADMSG + "\t" + rt + "\t" + node.getNodeId() + "\t" + newnodeid));
								}
							}
						}
					}
					
					reporter.incrCounter("Contrail", "totalthreadedbp", totalthreadedbp);
				}
			}

			if (print_node)
			{
				output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
			}
		}
	} 

//	ThreadResolveReducer
///////////////////////////////////////////////////////////////////////////

	private static class ThreadResolveReducer extends MapReduceBase 
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
			Map<String, List<String>> updates = new HashMap<String, List<String>>();

			int sawnode = 0;
			
			//V = node.getNodeId().equals("GMSRRSDLCJGRDHA");

			while(iter.hasNext())
			{
				String msg = iter.next().toString();

				if (V) { System.err.println("thread resolve reduce: " + nodeid.toString() + "\t" + msg); }

				String [] vals = msg.split("\t");

				if (vals[0].equals(Node.NODEMSG))
				{
					node.parseNodeMsg(vals, 0);
					sawnode++;
				}
				else if (vals[0].equals(Node.RESOLVETHREADMSG))
				{
					String dir = vals[1];
					String oid = vals[2];
					String nid = vals[3];

					String link = dir + ":" + oid;

					if (updates.containsKey(link))
					{
						updates.get(link).add(nid);
					}
					else
					{
						List<String> nids = new ArrayList<String>();
						nids.add(nid);
						updates.put(link, nids);
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

			if (updates.size() > 0)
			{
				Set<String> resolved = new HashSet<String>();

				// Remove the links to the split node, and add new link(s)
				for(String up : updates.keySet())
				{
					String [] vals = up.split(":");
					String dir = vals[0];
					String oid = vals[1];

					node.removelink(oid, dir);
					resolved.add(up);

					List<String> nids = updates.get(up);
					for (String nid : nids)
					{
						node.addEdge(dir, nid);
					}
				}

				// Update the threading reads now that the link is resolved
				List<String> threads = node.getThreads();

				if (threads != null)
				{
					int threadcnt = threads.size();
					for (int i = 0; i < threadcnt; i++)
					{
						String thread = threads.get(i);
						String [] vals = thread.split(":");
						String tdir = vals[0];
						String tn   = vals[1];
						String read = vals[2];

						String key = tdir + ":" + tn;

						if (updates.containsKey(key))
						{

							// It is possible the old link was split into multiple
							// We can only resolve unambiguous cases

							List<String> nids = updates.get(key);

							if (nids.size() == 1)
							{
								//print STDERR "Update thread: $thread";
								String nid = nids.get(0);
								thread = tdir + ":" + nid + ":" + read;
								threads.set(i, thread);
							}
						}
					}

					node.cleanThreads();
				}

				// Cleanup the threadible msgs
				List<String> threadmsgs = node.getThreadibleMsgs();

				if (threadmsgs != null)
				{
					node.clearThreadibleMsg();
					for (String k : threadmsgs)
					{
						if (!resolved.contains(k))
						{
							node.addThreadibleMsg(k);
						}
					}
				}
			}

			output.collect(nodeid, new Text(node.toNodeMsg()));
		}
	}



//	Run Tool
///////////////////////////////////////////////////////////////////////////	

	public RunningJob run(String inputPath, String outputPath) throws Exception
	{ 
		sLogger.info("Tool name: ThreadResolve");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(Stats.class);
		conf.setJobName("ThreadResolve " + inputPath + " " + ContrailConfig.K);
		
		ContrailConfig.initializeConfiguration(conf);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(ThreadResolveMapper.class);
		conf.setReducerClass(ThreadResolveReducer.class);

		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return JobClient.runJob(conf);
	}


//	Parse Arguments and run
///////////////////////////////////////////////////////////////////////////	

	public int run(String[] args) throws Exception 
	{
		String inputPath  = "/Users/mschatz/contrail/Ec100k/11-scaffold.1.threadible";
		String outputPath = "/users/mschatz/contrail/Ec100k/11-scaffold.1.resolved";
		
		ContrailConfig.K = 25;

		run(inputPath, outputPath);
		return 0;
	}


//	Main
///////////////////////////////////////////////////////////////////////////	

	public static void main(String[] args) throws Exception 
	{
		int res = ToolRunner.run(new Configuration(), new ThreadResolve(), args);
		System.exit(res);
	}
}
