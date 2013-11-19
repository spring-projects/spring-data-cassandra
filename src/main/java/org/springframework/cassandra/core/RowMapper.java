package org.springframework.cassandra.core;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.DriverException;

public interface RowMapper<T> {

	T mapRow(Row row, int rowNum) throws DriverException;

}
