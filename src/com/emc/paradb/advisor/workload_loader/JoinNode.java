package com.emc.paradb.advisor.workload_loader;

/**
 * this class is not used now
 * it is designed to store join operations info
 * @author xpan
 *
 */
public class JoinNode {
	
	String leftTable;
	String rightTable;
	String leftKey;
	String rightKey;
	
	public String getLeftTable() {
		return leftTable;
	}
	public void setLeftTable(String leftTable) {
		this.leftTable = leftTable;
	}
	public String getRightTable() {
		return rightTable;
	}
	public void setRightTable(String rightTable) {
		this.rightTable = rightTable;
	}
	public String getLeftKey() {
		return leftKey;
	}
	public void setLeftKey(String leftKey) {
		this.leftKey = leftKey;
	}
	public String getRightKey() {
		return rightKey;
	}
	public void setRightKey(String rightKey) {
		this.rightKey = rightKey;
	}

}
