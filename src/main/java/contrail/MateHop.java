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


public class MateHop extends Configured implements Tool 
{	
	private static final Logger sLogger = Logger.getLogger(MateHop.class);
	public static boolean V = false;


	// MateHopMapper
	///////////////////////////////////////////////////////////////////////////

	private static class MateHopMapper extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, Text> 
	{
		private static int K = 0;
		private static boolean FIRST_HOP = false;

		public void configure(JobConf job) 
		{
			K = Integer.parseInt(job.get("K"));
			FIRST_HOP = Integer.parseInt(job.get("FIRST_HOP")) == 1;
		}

		public void print_hopmsg(Node node, String path, String dest, 
						         String curdist, String outdir, 
						         String expdist, String expdir,
						         OutputCollector<Text, Text> output,
						         Reporter reporter) throws IOException
		{
			// Looking for a path to $ctg in the ud direction
			int matethreadmsgs = 0;
			
			String startnode = null;
			int pos = path.indexOf(':');
			if (pos > 0)
			{
				startnode = path.substring(0, pos);
				//System.err.println("Avoid selfloops to " + startnode + " in " + path);
			}

			for (String curdir : Node.dirs)
			{
				String tt = outdir + curdir;

				List<String> edges = node.getEdges(tt);

				if (edges != null)
				{
					for (String v : edges)
					{
						if ((startnode != null) && (v.equals(startnode)))
						{
							if (V) { System.err.println("Found self loop: " + path + " to " + v); }
							reporter.incrCounter("Contrail", "selfloop", 1);
						}
						else
						{
							output.collect(new Text(v), 
								new Text(Node.MATETHREAD + "\t" + path + ":" + tt + "\t" +
										curdist + "\t" + curdir + "\t" + 
										expdist + "\t" + expdir + "\t" +
										dest));
							matethreadmsgs++;
						}
					}
				}
			}

			reporter.incrCounter("Contrail", "matethreadmsgs", matethreadmsgs);
		}


		public void map(LongWritable lineid, Text nodetxt,
				OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException 
		{
			Node node = new Node();
			node.fromNodeMsg(nodetxt.toString());

			if (FIRST_HOP)
			{
				List<String> bundles = node.getBundles();

				if (bundles != null)
				{
					int curdist = -K+1;

					for(String bstr : bundles)
					{
						String [] vals = bstr.split(":");
						String dest     = vals[0];
						String edgetype = vals[1];
						String expdist  = vals[2];
						//String weight   = vals[3];
						//String unique   = vals[4];

						String outdir = edgetype.substring(0, 1);
						String expdir = edgetype.substring(1, 2);

						print_hopmsg(node, node.getNodeId(), dest,
								Integer.toString(curdist), outdir,
								expdist, expdir,
								output, reporter);
					}
				}
			}
			else
			{
				List<String> matethreads = node.getMateThreads();
				if (matethreads != null)
				{
					for (String hopmsg : matethreads)
					{
						String [] vals = hopmsg.split("%");
						String path    = vals[0];
						String curdist = vals[1];
						String outdir  = vals[2];
						String expdist = vals[3];
						String expdir  = vals[4];
						String dest    = vals[5];

						print_hopmsg(node, path, dest,
								curdist, outdir,
								expdist, expdir,
								output, reporter);
					}

					node.clearMateThreads();
				}
			}


			output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
			reporter.incrCounter("Contrail", "nodes", 1);
		}
	}

	// MateHopReducer
	///////////////////////////////////////////////////////////////////////////

	private static class MateHopReducer extends MapReduceBase 
	implements Reducer<Text, Text, Text, Text> 
	{
		private static int K = 0;
		private static int INSERT_LEN = 0;
		private static int MIN_WIGGLE = 0;
		private static long wiggle = 0;

		public void configure(JobConf job) {
			K = Integer.parseInt(job.get("K"));
			INSERT_LEN = Integer.parseInt(job.get("INSERT_LEN"));
			MIN_WIGGLE = Integer.parseInt(job.get("MIN_WIGGLE"));
			wiggle = Node.mate_wiggle(INSERT_LEN, MIN_WIGGLE);
		}
		
		
		public class Hop
		{
			String path;
			int curdist;
			String curdir;
		    int expdist;
		    String expdir;
		    String dest;
		    
		    public Hop(String[] vals, int offset)
		    {
		    	path    = vals[offset];
		    	curdist = Integer.parseInt(vals[offset+1]);
		    	curdir  = vals[offset+2];
		    	expdist = Integer.parseInt(vals[offset+3]);
		    	expdir  = vals[offset+4];
		    	dest    = vals[offset+5];
		    }
		    
		    public String toString()
		    {
		    	return dest + " " + expdist + expdir + " | cur: " + curdist + curdir + " " + path;
		    }
		}

		public void reduce(Text nodeid, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException 
		{
			Node node = new Node(nodeid.toString());
			
			List<Hop> hops = new ArrayList<Hop>();

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
				else if (vals[0].equals(Node.MATETHREAD))
				{
					Hop h = new Hop(vals, 1);
					hops.add(h);
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
			
			long foundshort   = 0;
			long foundlong    = 0;
			long foundinvalid = 0;
			long foundvalid   = 0;
			long active       = 0;
			long toolong      = 0;
			
			List<String> bundles = node.getBundles();
			
			if (bundles != null)
			{
				for (String bstr : bundles)
				{
					if (bstr.charAt(0) == '#')
					{
						// There is a valid path already saved away
				        foundvalid++;
					}
				}
			}
			
			if (hops.size() > 0)
			{
				for (Hop h : hops)
				{
					if (V) { System.err.println("Checking : " + h.toString()); }

					if (h.dest.equals(node.getNodeId()))
					{
						if (h.curdist < h.expdist - wiggle)
						{
							if (V) { System.err.println("Found too short: " + h.curdist + " exp: " + h.expdist + " " + h.path); }
							
							foundshort++;
							continue; // don't try to extend this search, because the dest node is supposed to be unique
						}

						if (h.curdist > h.expdist + 2*wiggle)
						{
							if (V) { System.err.println("Found too long: " + h.curdist + " exp: " + h.expdist + " " + h.path); }
							foundlong++;
							continue;
						}

						if (!h.curdir.equals(h.expdir))
						{
							if (V) { System.err.println("Found invalid"); }
							foundinvalid++;
							continue;
						}

						// Success!
						foundvalid++;

						String pp = "#" + h.path + ":" + node.getNodeId();
						node.addBundle(pp);

						if (V) { System.err.println("Found valid path: " + pp); }
					}
					else
					{
						if (h.curdist > h.expdist + 2*wiggle)
						{
							if (V) { System.err.println("too long: " + h.curdist + " exp: " + h.expdist + " " + h.path); }
							
							toolong++;
							continue;
						}

						// The current path is still active, save away for next hop
						int curdist = h.curdist + node.len() - K + 1;
						String path = h.path + ":" + node.getNodeId();

						String msg = path      + "%" +
						curdist   + "%" +
						h.curdir  + "%" +
						h.expdist + "%" +
						h.expdir  + "%" +
						h.dest;

						if (V) { System.err.println("Keep searching: " + msg); }

						node.addMateThread(msg);
						active++;
					}
				}
			}

			output.collect(nodeid, new Text(node.toNodeMsg()));
			
			reporter.incrCounter("Contrail", "foundshort",   foundshort);
			reporter.incrCounter("Contrail", "foundlong",    foundlong);
			reporter.incrCounter("Contrail", "foundinvalid", foundinvalid);
			reporter.incrCounter("Contrail", "foundvalid",   foundvalid);
			reporter.incrCounter("Contrail", "active",       active);
			reporter.incrCounter("Contrail", "toolong",      toolong);
		}
	}


	// Run Tool
	///////////////////////////////////////////////////////////////////////////	

	public RunningJob run(String inputPath, String outputPath, boolean isfirst) throws Exception
	{ 
		sLogger.info("Tool name: MateHop");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);
		sLogger.info(" - isfirst: " + isfirst);

		JobConf conf = new JobConf(Stats.class);
		conf.setJobName("MateHop " + inputPath);
		
		ContrailConfig.initializeConfiguration(conf);
		conf.setLong("FIRST_HOP", (isfirst ? 1 : 0));

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(MateHopMapper.class);
		conf.setReducerClass(MateHopReducer.class);

		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return JobClient.runJob(conf);
	}


	// Parse Arguments and run
	///////////////////////////////////////////////////////////////////////////	

	public int run(String[] args) throws Exception 
	{
		String inputPath  = "/Users/mschatz/try/11-scaffold.1.bundles";
		String outputPath = "/users/mschatz/try/11-scaffold.1.search1";

		boolean isfirst = true;
		
		ContrailConfig.INSERT_LEN = 100;
		ContrailConfig.K = 25;

		run(inputPath, outputPath, isfirst);
		return 0;
	}


	// Main
	///////////////////////////////////////////////////////////////////////////	

	public static void main(String[] args) throws Exception 
	{
		int res = ToolRunner.run(new Configuration(), new MateHop(), args);
		System.exit(res);
	}
}
