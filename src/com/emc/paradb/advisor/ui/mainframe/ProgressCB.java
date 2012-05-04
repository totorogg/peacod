package com.emc.paradb.advisor.ui.mainframe;

/**
 * process bar call back interface, for displaying process status
 * @author xpan
 *
 */
public interface ProgressCB
{
	//displaying progress
	public void setProgress(int progress);
	//displaying states
	public void setState(String state);
}