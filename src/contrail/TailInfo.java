package contrail;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TailInfo 
{
	public String id;
	public String dir;
	public int    dist;
	
	public TailInfo(TailInfo o)
	{
		id   = o.id;
		dir  = o.dir;
		dist = o.dist;
	}
	
	public TailInfo()
	{
		id = null;
		dir = null;
		dist = 0;
	}
	
	public String toString()
	{
		if (this == null)
		{
			return "null";
		}
		
		return id + " " + dir + " " + dist;
	}
	
	public static TailInfo find_tail(Map<String, Node> nodes, Node startnode, String startdir) throws IOException
	{
		//System.err.println("find_tail: " + startnode.getNodeId() + " " + startdir);
		Set<String> seen = new HashSet<String>();
		seen.add(startnode.getNodeId());
		
		Node curnode = startnode;
		String curdir = startdir;
		String curid = startnode.getNodeId();
		int dist = 0;
		
		boolean canCompress = false;
		
		do
		{
			canCompress = false;
			
			TailInfo next = curnode.gettail(curdir);
			
			//System.err.println(curnode.getNodeId() + " " + curdir + ": " + next);

			if ((next != null) &&
				(nodes.containsKey(next.id)) &&
				(!seen.contains(next.id)))
			{
				seen.add(next.id);
				curnode = nodes.get(next.id);

				TailInfo nexttail = curnode.gettail(Node.flip_dir(next.dir));
				
				if ((nexttail != null) && (nexttail.id.equals(curid)))
				{
					dist++;
					canCompress = true;
					
					curid = next.id;
					curdir = next.dir;
				}
			}
		}
		while (canCompress);

		TailInfo retval = new TailInfo();
		
		retval.id = curid;
		retval.dir = Node.flip_dir(curdir);
		retval.dist = dist;
			
		return retval;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
