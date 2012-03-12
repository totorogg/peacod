package com.emc.paradb.advisor.algorithm;

/**
 * a util class provides the round robin function
 * @author Xin Pan
 *
 */
public class RoundRobin
{
	private int nodes = 0;
	public RoundRobin(int nodes)
	{
		this.nodes = nodes;
	}
	/**
	 * we define the round robin as value % node, a special case of hash
	 * @return
	 */
	public int getPlacement(String value)
	{
		int node = 0;
		try
		{
			node = Integer.valueOf(value) % nodes;
		}
		catch(NumberFormatException e)
		{
			node = value.hashCode() % nodes;
		}
		return node;		
	}
}