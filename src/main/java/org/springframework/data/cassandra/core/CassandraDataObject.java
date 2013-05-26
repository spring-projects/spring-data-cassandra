package org.springframework.data.cassandra.core;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.springframework.core.convert.ConversionException;
import org.springframework.data.cassandra.core.convert.CassandraConversionException;


public class CassandraDataObject {
	Map<String, Map<String,String>> dataMap = new HashMap<String, Map<String,String>>();
	
	public void clear() {
		dataMap.clear();
	}

	public boolean containsKey(Object key) {
		return dataMap.containsKey(key);
	}

	public Map<String, String> getRow(String rowKey) {
		return dataMap.get(rowKey);
	}

	public Map<String, String> getOnlyRow() throws ConversionException{
		if (dataMap.size() != 1){
			throw new CassandraConversionException("Requested only row, but had [" + dataMap.size() + "]");
		}
		Collection<Map<String, String>> columns = dataMap.values();
		
		if (columns == null){
			throw new CassandraConversionException("No columns found for only row.");
		}
		return columns.iterator().next();		
	}

	public boolean isEmpty() {
		return dataMap.isEmpty();
	}

	public void putRow(String rowKey, Map<String, String> columns) {
		dataMap.put(rowKey, columns);
	}

	public void removeRow(Object rowKey) {
		this.dataMap.remove(rowKey);
	}

	public int numRows() {
		return this.dataMap.size();
	}
}
