package com.emc.paradb.advisor.algorithm;


public class RoundRobin
{
	private int nodes = 0;
	private int curNode = 0;
	
	public RoundRobin(int nodes)
	{
		this.nodes = nodes;
	}
	/**
	 * the RoundRobin class will record the current node
	 * the next time you call getPlacement(), it will return (current node + 1)%nodes
	 * @return
	 */
	public int getPlacement()
	{
		return (curNode++) % nodes;
	}
}