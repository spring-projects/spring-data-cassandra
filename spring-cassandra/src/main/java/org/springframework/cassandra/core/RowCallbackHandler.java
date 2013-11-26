package org.springframework.cassandra.core;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.DriverException;

public interface RowCallbackHandler {

	void processRow(Row row) throws DriverException;

}
