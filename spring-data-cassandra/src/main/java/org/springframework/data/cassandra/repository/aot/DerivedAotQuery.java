package org.springframework.data.cassandra.repository.aot;

import java.util.List;

import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.repository.query.ParameterBinding;
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * PartTree (derived) Query with a limit associated.
 *
 * @author Mark Paluch
 * @since 5.0
 */
class DerivedAotQuery extends AotQuery {

	private final String queryString;
	private final Query query;
	private final Sort sort;
	private final Limit limit;
	private final boolean delete;
	private final boolean count;
	private final boolean exists;

	DerivedAotQuery(String queryString, List<ParameterBinding> bindings, Query query, PartTree partTree) {

		this(queryString, bindings, query, partTree.getSort(), partTree.getResultLimit(), partTree.isDelete(),
				partTree.isCountProjection(), partTree.isExistsProjection());
	}

	private DerivedAotQuery(String queryString, List<ParameterBinding> bindings, Query query, Sort sort, Limit limit,
			boolean delete, boolean count, boolean exists) {

		super(bindings);

		this.queryString = queryString;
		this.query = query;
		this.sort = sort;
		this.limit = limit;
		this.delete = delete;
		this.count = count;
		this.exists = exists;
	}

	public String getQueryString() {
		return queryString;
	}

	@Override
	public Limit getLimit() {
		return limit;
	}

	@Override
	public boolean isDelete() {
		return delete;
	}

	@Override
	public boolean isCount() {
		return count;
	}

	@Override
	public boolean isExists() {
		return exists;
	}

	public Query getQuery() {
		return query;
	}

	public Sort getSort() {
		return sort;
	}
}
