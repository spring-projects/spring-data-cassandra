package org.springframework.cassandra.core.cql.builder;

import static org.springframework.cassandra.core.cql.CqlStringUtils.escapeSingle;
import static org.springframework.cassandra.core.cql.CqlStringUtils.noNull;
import static org.springframework.cassandra.core.cql.CqlStringUtils.singleQuote;

import java.util.Map;

import org.springframework.cassandra.core.keyspace.AbstractTableSpecification;
import org.springframework.cassandra.core.keyspace.Option;
import org.springframework.util.Assert;

/**
 * Base class that contains behavior common to table operations.
 * 
 * @author Matthew T. Adams
 * @param T The subtype of AbstractTableSpecification for which this is a CQL generator.
 */
public abstract class AbstractTableOperationCqlGenerator<T extends AbstractTableSpecification<T>> {

	protected abstract StringBuilder toCql(StringBuilder cql);

	private AbstractTableSpecification<T> specification;

	public AbstractTableOperationCqlGenerator(AbstractTableSpecification<T> specification) {
		setSpecification(specification);
	}

	protected void setSpecification(AbstractTableSpecification<T> specification) {
		Assert.notNull(specification);
		this.specification = specification;
	}

	@SuppressWarnings("unchecked")
	public T getSpecification() {
		return (T) specification;
	}

	/**
	 * Convenient synonymous method of {@link #getSpecification()}.
	 */
	protected T spec() {
		return getSpecification();
	}

	protected StringBuilder optionValueMap(Map<Option, Object> valueMap, StringBuilder cql) {
		cql = noNull(cql);

		if (valueMap == null || valueMap.isEmpty()) {
			return cql;
		}
		// else option value is a non-empty map

		// append { 'name' : 'value', ... }
		cql.append("{ ");
		boolean mapFirst = true;
		for (Map.Entry<Option, Object> entry : valueMap.entrySet()) {
			if (mapFirst) {
				mapFirst = false;
			} else {
				cql.append(", ");
			}

			Option option = entry.getKey();
			cql.append(singleQuote(option.getName())); // entries in map keys are always quoted
			cql.append(" : ");
			Object entryValue = entry.getValue();
			entryValue = entryValue == null ? "" : entryValue.toString();
			if (option.escapesValue()) {
				entryValue = escapeSingle(entryValue);
			}
			if (option.quotesValue()) {
				entryValue = singleQuote(entryValue);
			}
			cql.append(entryValue);
		}
		cql.append(" }");

		return cql;
	}

	public String toCql() {
		return toCql(null).toString();
	}
}
