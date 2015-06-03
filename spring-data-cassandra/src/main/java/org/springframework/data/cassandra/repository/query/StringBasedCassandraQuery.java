package org.springframework.data.cassandra.repository.query;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.core.CassandraOperations;

public class StringBasedCassandraQuery extends AbstractCassandraQuery {

	private static final Pattern PLACEHOLDER = Pattern.compile("\\?(\\d+)");
	private static final Logger LOG = LoggerFactory.getLogger(StringBasedCassandraQuery.class);

	protected String query;

	public StringBasedCassandraQuery(String query, CassandraQueryMethod queryMethod, CassandraOperations operations) {

		super(queryMethod, operations);

		this.query = query;
	}

	public StringBasedCassandraQuery(CassandraQueryMethod queryMethod, CassandraOperations operations) {
		this(queryMethod.getAnnotatedQuery(), queryMethod, operations);
	}

	@Override
	public String createQuery(CassandraParameterAccessor accessor, CassandraConverter converter) {
		return replacePlaceholders(query, accessor, converter);
	}

	private String replacePlaceholders(String input, CassandraParameterAccessor accessor, CassandraConverter converter) {

		Matcher matcher = PLACEHOLDER.matcher(input);
		String result = input;

		while (matcher.find()) {
			String group = matcher.group();
			int index = Integer.parseInt(matcher.group(1));
			Object value = getParameterWithIndex(accessor, index);
			String stringValue = null;
			CassandraQueryMethod queryMethod = getQueryMethod();

			stringValue = queryMethod.convertParameterToCQL(index, value, converter);
			
			result = result.replace(group, stringValue);
		}

		return result;
	}

	private Object getParameterWithIndex(CassandraParameterAccessor accessor, int index) {
		return accessor.getBindableValue(index);
	}
}
