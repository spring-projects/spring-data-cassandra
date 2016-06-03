package org.springframework.cassandra.test.integration.core.template.async;

import org.springframework.cassandra.core.QueryForObjectListener;
import org.springframework.cassandra.test.unit.support.TestListener;

public class ObjectListener<T> extends TestListener implements QueryForObjectListener<T> {

	public T result;
	public Exception exception;

	@Override
	public void onQueryComplete(T result) {
		this.result = result;
		countDown();
	}

	@Override
	public void onException(Exception x) {
		this.exception = x;
		countDown();
	}
}
