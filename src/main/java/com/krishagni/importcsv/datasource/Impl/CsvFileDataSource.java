package com.krishagni.importcsv.datasource.Impl;

import com.krishagni.catissueplus.core.common.util.CsvFileReader;
import com.krishagni.importcsv.core.Record;
import com.krishagni.importcsv.datasource.DataSource;

public class CsvFileDataSource implements DataSource {
	private CsvFileReader csvReader;
	
	public CsvFileDataSource(String filename) {
		this.csvReader = CsvFileReader.createCsvFileReader(filename, true);
	}
	
	@Override
	public Record nextRecord() {
		return getRecord(csvReader.getColumnNames(), csvReader.getRow());
	}
	
	@Override
	public boolean hasNext() {
		return csvReader.next();
	}
	
	@Override
	public String[] getHeader() {
		return (csvReader.getColumnNames());
	}
	
	@Override
	public void close() {
		csvReader.close();
	}
	
	private Record getRecord(String[] columnNames, String[] row) {
		Record record = new Record();
		for (int i = 0; i <row.length; i++) {
			record.addValue(columnNames[i], row[i]);
		}
		return record;
	}
}