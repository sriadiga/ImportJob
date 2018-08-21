package com.krishagni.importcsv.datasource;

import com.krishagni.importcsv.core.Record;

public interface DataSource {
	Record nextRecord();
	
	boolean hasNext();
	
	void close();
	
	String[] getHeader();
}