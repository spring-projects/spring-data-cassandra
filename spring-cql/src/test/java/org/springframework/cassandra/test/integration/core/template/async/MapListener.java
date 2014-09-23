package org.springframework.cassandra.test.integration.core.template.async;

import java.util.Map;

import org.springframework.cassandra.core.QueryForMapListener;
import org.springframework.cassandra.test.unit.support.TestListener;

public class MapListener extends TestListener implements QueryForMapListener {

	Map<String, Object> result;
	Exception exception;

	@Override
	public void onQueryComplete(Map<String, Object> results) {
		countDown();
		this.result = results;
	}

	@Override
	public void onException(Exception x) {
		countDown();
		this.exception = x;
	}
}
