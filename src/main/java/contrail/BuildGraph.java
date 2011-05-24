package contrail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.List;
import java.util.Map;

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


public class BuildGraph extends Configured implements Tool 
{	
	private static final Logger sLogger = Logger.getLogger(BuildGraph.class);
	
	private static class BuildGraphMapper extends MapReduceBase 
    implements Mapper<LongWritable, Text, Text, Text> 
	{
		private static int K = 0;
		private static int TRIM5 = 0;
		private static int TRIM3 = 0;
		
		public void configure(JobConf job) 
		{
			K = Integer.parseInt(job.get("K"));
			TRIM5 = Integer.parseInt(job.get("TRIM5"));
			TRIM3 = Integer.parseInt(job.get("TRIM3"));
		}
		
		public void map(LongWritable lineid, Text nodetxt,
				        OutputCollector<Text, Text> output, Reporter reporter)
		                throws IOException 
		{
			String[] fields = nodetxt.toString().split("\t");
			
			if (fields.length != 2)
			{
				//System.err.println("Warning: invalid input: \"" + nodetxt.toString() + "\"");
				reporter.incrCounter("Contrail", "input_lines_invalid", 1);
				return;
			}

			String tag = fields[0];
			
			tag.replaceAll(" ", "_");
			tag.replaceAll(":", "_");
			tag.replaceAll("#", "_");
			tag.replaceAll("-", "_");
			tag.replaceAll(".", "_");

			String seq = fields[1].toUpperCase();

			// Hard chop a few bases off of each end of the read
			if (TRIM5 > 0 || TRIM3 > 0)
			{
				// System.err.println("orig: " + seq);
				seq = seq.substring(TRIM5, seq.length() - TRIM5 - TRIM3);
				// System.err.println("trim: " + seq);
			}

			// Automatically trim Ns off the very ends of reads
			int endn = 0;
			while (endn < seq.length() && seq.charAt(seq.length()-1-endn) == 'N') { endn++; }
			if (endn > 0) { seq = seq.substring(0, seq.length()-endn); }
			
			int startn = 0;
			while (startn < seq.length() && seq.charAt(startn) == 'N') { startn++; }
			if (startn > 0) { seq = seq.substring(startn, seq.length() - startn); }

			// Check for non-dna characters
			if (seq.matches(".*[^ACGT].*"))
			{
				//System.err.println("WARNING: non-DNA characters found in " + tag + ": " + seq);
				reporter.incrCounter("Contrail", "reads_skipped", 1);	
				return;
			}

			// check for short reads
			if (seq.length() <= K)
			{
				//System.err.println("WARNING: read " + tag + " is too short: " + seq);
				reporter.incrCounter("Contrail", "reads_short", 1);	
				return;
			}

			// Now emit the edges of the de Bruijn Graph

			char ustate = '5';
			char vstate = 'i';

			Set<String> seenmers = new HashSet<String>();

			String chunkstr = "";
			int chunk = 0;

			int end = seq.length() - K;
			
			for (int i = 0; i < end; i++)
			{
				String u = seq.substring(i,   i+K);
				String v = seq.substring(i+1, i+1+K);
				
				String f = seq.substring(i, i+1);
				String l = seq.substring(i+K, i+K+1);
				f = Node.rc(f);

				char ud = Node.canonicaldir(u);
				char vd = Node.canonicaldir(v);

				String t  = Character.toString(ud) + vd;
				String tr = Node.flip_link(t);
				
				String uc0 = Node.canonicalseq(u);
				String vc0 = Node.canonicalseq(v);

				String uc = Node.str2dna(uc0);
				String vc = Node.str2dna(vc0);
				
				//System.out.println(u + " " + uc0 + " " + ud + " " + uc);
				//System.out.println(v + " " + vc0 + " " + vd + " " + vc);
				
				if ((i == 0) && (ud == 'r'))  { ustate = '6'; }
				if (i+1 == end) { vstate = '3'; }

				boolean seen = (seenmers.contains(u) || seenmers.contains(v) || u.equals(v));
				seenmers.add(u);

				if (seen)
				{
					chunk++;
					chunkstr = "c" + chunk;
					//#print STDERR "repeat internal to $tag: $uc u$i $chunk\n";
				}

				//System.out.println(uc + "\t" + t + "\t" + l + "\t" + tag + chunkstr + "\t" + ustate);
				
				output.collect(new Text(uc), 
						       new Text(t + "\t" + l + "\t" + tag + chunkstr + "\t" + ustate));

				if (seen)
				{
					chunk++;
					chunkstr = "c" + chunk;
					//#print STDERR "repeat internal to $tag: $vc v$i $chunk\n";
				}

				//print "$vc\t$tr\t$f\t$tag$chunk\t$vstate\n";
				
				//System.out.println(vc + "\t" + tr + "\t" + f + "\t" + tag + chunkstr + "\t" + vstate);
				
				output.collect(new Text(vc), 
						new Text(tr + "\t" + f + "\t" + tag + chunkstr + "\t" + vstate));

				ustate = 'm';
			}
			
			reporter.incrCounter("Contrail", "reads_good", 1);
			reporter.incrCounter("Contrail", "reads_goodbp", seq.length());
		}			
	}
	
	private static class BuildGraphReducer extends MapReduceBase 
	implements Reducer<Text, Text, Text, Text> 
	{
		private static int K = 0;
		private static int MAXTHREADREADS = 0;
		private static int MAXR5 = 0;
		private static boolean RECORD_ALL_THREADS = false;

		public void configure(JobConf job) {
			K = Integer.parseInt(job.get("K"));
			MAXTHREADREADS = Integer.parseInt(job.get("MAXTHREADREADS"));
			MAXR5 = Integer.parseInt(job.get("MAXR5"));
			RECORD_ALL_THREADS = Integer.parseInt(job.get("RECORD_ALL_THREADS")) == 1;
		}

		public void reduce(Text curnode, Iterator<Text> iter,
						   OutputCollector<Text, Text> output, Reporter reporter)
						   throws IOException 
		{
			Node node = new Node();
			
			String mertag = null;
			float cov = 0;
			
			Map<String, Map<String, List<String>>> edges = new HashMap<String, Map<String, List<String>>>();
			
			while(iter.hasNext())
			{
				String valstr = iter.next().toString();
				String [] vals = valstr.split("\t");

				String type     = vals[0]; // edge type between mers
				String neighbor = vals[1]; // id of neighboring node
				String tag      = vals[2]; // id of read contributing to edge
				String state    = vals[3]; // internal or end mer
				
				// Add the edge to the neighbor
				Map<String, List<String>> neighborinfo = null;
				if (edges.containsKey(type))
				{
					neighborinfo = edges.get(type);
				}
				else
				{
					neighborinfo = new HashMap<String, List<String>>();
					edges.put(type, neighborinfo);
				}
				
				
				// Now record the read supports the edge
				List<String> tags = null;
				if (neighborinfo.containsKey(neighbor))
				{
					tags = neighborinfo.get(neighbor);
				}
				else
				{
					tags = new ArrayList<String>();
					neighborinfo.put(neighbor, tags);
				}
				
				if (tags.size() < MAXTHREADREADS)
				{
					tags.add(tag);
				}
				
				// Check on the mertag
				if (mertag == null || (tag.compareTo(mertag) < 0))
				{
					mertag = tag;
				}

				// Update coverage, offsets
				if (!state.equals("i"))
				{
					cov++;

					if (state.equals("6"))
					{
						node.addR5(tag, K-1, 1, MAXR5);
					}
					else if (state.equals("5"))
					{
						node.addR5(tag, 0, 0, MAXR5);
					}
				}
			}

			node.setMertag(mertag);
			node.setCoverage(cov);
			
			String seq = Node.dna2str(curnode.toString());
			String rc  = Node.rc(seq);
			
			node.setstr_raw(curnode.toString());

			seq = seq.substring(1);
			rc  = rc.substring(1);
			
			char [] dirs = {'f', 'r'};
			
			for (int d = 0; d < 2; d++)
			{
				String x = Character.toString(dirs[d]); 

				int degree = 0;

				for (int e = 0; e < 2; e++)
				{
					String t = x + dirs[e];

					if (edges.containsKey(t))
					{
						degree += edges.get(t).size();
					}
				}

				for(int e = 0; e < 2; e++)
				{
					String t = x + dirs[e];

					if (edges.containsKey(t))
					{
						Map<String, List<String>> edgeinfo = edges.get(t);

						for (String vc : edgeinfo.keySet())
						{
							String v = seq;
							if (dirs[d] == 'r') { v = rc; }

							v = v +  vc;

							if (dirs[e] == 'r') { v = Node.rc(v); }

							String link = Node.str2dna(v);

							node.addEdge(t, link);

							if ((degree > 1) || RECORD_ALL_THREADS)
							{
								for (String r : edgeinfo.get(vc))
								{
									node.addThread(t, link, r);
								}
							}
						}
					}
				}
			}

			output.collect(curnode, new Text(node.toNodeMsg()));
			reporter.incrCounter("Contrail", "nodecount", 1);
		}
	}

		
	
	public RunningJob run(String inputPath, String outputPath) throws Exception
	{
		sLogger.info("Tool name: BuildGraph");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);
		
		JobConf conf = new JobConf(Stats.class);
		conf.setJobName("BuildGraph " + inputPath + " " + ContrailConfig.K);
		
		ContrailConfig.initializeConfiguration(conf);
			
		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(BuildGraphMapper.class);
		conf.setReducerClass(BuildGraphReducer.class);

		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return JobClient.runJob(conf);
	}
	
	public int run(String[] args) throws Exception 
	{
		String inputPath  = "/Users/mschatz/build/Contrail/data/B.anthracis.36.50k.sfa";
		String outputPath = "/users/mschatz/try/build";
		ContrailConfig.K = 21;
		
		long starttime = System.currentTimeMillis();
		
		run(inputPath, outputPath);
		
		long endtime = System.currentTimeMillis();
		
		float diff = (float) (((float) (endtime - starttime)) / 1000.0);
		
		System.out.println("Runtime: " + diff + " s");
		
		return 0;
	}

	public static void main(String[] args) throws Exception 
	{
		int res = ToolRunner.run(new Configuration(), new BuildGraph(), args);
		System.exit(res);
	}
}
