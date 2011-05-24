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


public class MateBundle extends Configured implements Tool 
{	
	private static final Logger sLogger = Logger.getLogger(MateBundle.class);
	
	public static boolean V = false;


	// MateBundleMapper
	///////////////////////////////////////////////////////////////////////////

	private static class MateBundleMapper extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, Text> 
	{
		public void map(LongWritable lineid, Text nodetxt,
				OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException 
		{
			String [] vals = nodetxt.toString().split("\t");

			StringBuffer buffer = new StringBuffer();

			boolean first = true;

			for(int offset = 1; offset < vals.length; offset++)
			{
				if (!first) { buffer.append("\t"); }
				first = false;
				buffer.append(vals[offset]);
			}

			output.collect(new Text(vals[0]), new Text(buffer.toString()));
		}
	}

	// MateBundleReducer
	///////////////////////////////////////////////////////////////////////////

	private static class MateBundleReducer extends MapReduceBase 
	implements Reducer<Text, Text, Text, Text> 
	{
		private static int K = 0;

		class ContigEdge
		{
			int dist;
			String basename;
			boolean unique;

			public ContigEdge(int curdist, String curbasename, boolean curunique)
			{
				dist = curdist;
				basename = curbasename;
				unique = curunique;
			}
		}

		public void reduce(Text nodeid, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException 
		{
			Node node = new Node(nodeid.toString());

			Map<String, List<ContigEdge>> edges = new HashMap<String, List<ContigEdge>>();

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
				else if (vals[0].equals(Node.MATEEDGE))
				{
					String edgetype = vals[1];
					String ctg      = vals[2];

					ContigEdge ce = new ContigEdge(Integer.parseInt(vals[3]), vals[4], Integer.parseInt(vals[5])==1);

					String key = edgetype + ":" + ctg;

					if (edges.containsKey(key))
					{
						edges.get(key).add(ce);
					}
					else
					{
						List<ContigEdge> elist = new ArrayList<ContigEdge>();
						elist.add(ce);
						edges.put(key, elist);
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

			for (String key : edges.keySet())
			{
				String [] vals = key.split(":");
				String edgetype = vals[0];
				String ctg = vals[1];

				// TODO: cluster consistent distances
				
				// For now bundle all edges to contig
				List<ContigEdge> elist = edges.get(key);

				int weight = elist.size();
				boolean unique = elist.get(0).unique;
				reporter.incrCounter("Contrail", "all_bundles", 1);

				if (unique)
				{
					reporter.incrCounter("Contrail", "unique_bundles", 1);

					double sum = 0;
					
					if (V) { System.err.print(node.getNodeId() + " " + ctg + " dists:"); }

					for(ContigEdge ce : elist)
					{
						if (V) { System.err.print(" " + ce.dist + ":" + ce.basename); }
						sum += ce.dist;
					}
					
					if (V) { System.err.println(); }

					int dist = (int) sum / weight;

					String bstr = ctg + ":" + edgetype + ":" + dist + ":" + weight + ":" + (unique ? "1" : "0");

					if (V) { System.err.println("Bundle " + node.getNodeId() + " " + bstr); }

					node.addBundle(bstr);
				}
			}

			output.collect(nodeid, new Text(node.toNodeMsg()));
		}
	}




	// Run Tool
	///////////////////////////////////////////////////////////////////////////	

	public RunningJob run(String graphPath, String edgePath, String outputPath) throws Exception
	{ 
		sLogger.info("Tool name: MateBundle");
		sLogger.info(" - graph: "  + graphPath);
		sLogger.info(" - edges: "  + edgePath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(Stats.class);
		conf.setJobName("MateBundle " + graphPath);
		
		ContrailConfig.initializeConfiguration(conf);

		FileInputFormat.addInputPath(conf, new Path(graphPath));
		FileInputFormat.addInputPath(conf, new Path(edgePath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(MateBundleMapper.class);
		conf.setReducerClass(MateBundleReducer.class);

		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return JobClient.runJob(conf);
	}


	// Parse Arguments and run
	///////////////////////////////////////////////////////////////////////////	

	public int run(String[] args) throws Exception 
	{
		String graphPath  = "/Users/mschatz/try/10-repeatscmp";
		String linkPath   = "/users/mschatz/try/11-scaffold.1.edges";
		String outputPath = "/users/mschatz/try/11-scaffold.1.bundles";
		ContrailConfig.K = 21;

		run(graphPath, linkPath, outputPath);
		return 0;
	}


	// Main
	///////////////////////////////////////////////////////////////////////////	

	public static void main(String[] args) throws Exception 
	{
		int res = ToolRunner.run(new Configuration(), new MateBundle(), args);
		System.exit(res);
	}
}
