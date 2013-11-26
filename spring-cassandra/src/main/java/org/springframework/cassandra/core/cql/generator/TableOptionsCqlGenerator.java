package org.springframework.cassandra.core.cql.generator;

import static org.springframework.cassandra.core.cql.CqlStringUtils.escapeSingle;
import static org.springframework.cassandra.core.cql.CqlStringUtils.noNull;
import static org.springframework.cassandra.core.cql.CqlStringUtils.singleQuote;

import java.util.Map;

import org.springframework.cassandra.core.keyspace.Option;
import org.springframework.cassandra.core.keyspace.TableOptionsSpecification;

/**
 * Base class that contains behavior common to CQL generation for table operations.
 * 
 * @author Matthew T. Adams
 * @param T The subtype of this class for which this is a CQL generator.
 */
public abstract class TableOptionsCqlGenerator<T extends TableOptionsSpecification<T>> extends
		TableNameCqlGenerator<TableOptionsSpecification<T>> {

	public TableOptionsCqlGenerator(TableOptionsSpecification<T> specification) {
		super(specification);
	}

	@SuppressWarnings("unchecked")
	protected T spec() {
		return (T) getSpecification();
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
}
