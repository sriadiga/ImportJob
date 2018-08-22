package com.krishagni.importcsv.core;

import java.util.HashMap;
import java.util.Map;

public class Record {
	private Map<String, String> record = new HashMap<String, String>();
	
	public String getValue(String column) {
		return record.get(column);
	}
	
	public void addValue(String column, String value) {
		record.put(column, value);
	}
}