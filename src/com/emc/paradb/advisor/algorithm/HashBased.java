package com.emc.paradb.advisor.algorithm;

/**
 * a util class provides the hash function
 * @author panx1
 *
 */
public class HashBased
{
	private int nodes = 0;
	
	public HashBased(int nodes)
	{
		this.nodes = nodes;
	}
	public int getPlacement(String keyValue)
	{
		return BKDRHash(keyValue) % nodes;
	}
	
	private int BKDRHash(String str){
		int seed = 131; // 31 131 1313 13131 131313 etc..
		int hash = 0;
		byte[] bytes = str.getBytes();
		for(int i = 0; i < bytes.length; i++)
		{
			hash = hash * seed + bytes[i];
		}
		return (hash & 0x7FFFFFFF);
	}
}