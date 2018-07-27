package org.springframework.data.cassandra.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.cql.WriteOptions;
import org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

public class ReactiveCassandraBatchTemplate implements ReactiveCassandraBatchOperations {

    private final AtomicBoolean                                    executed = new AtomicBoolean();
    private final ReactiveCassandraOperations                      operations;
    private final Batch                                            batch;
    private final List<Mono<Collection<? extends BuiltStatement>>> batchMonos;

    public ReactiveCassandraBatchTemplate(ReactiveCassandraOperations cassandraOperations) {
        this.operations = cassandraOperations;
        this.batch = QueryBuilder.batch();
        this.batchMonos = new ArrayList<>();
    }

    /* (non-Javadoc)
     * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#execute()
     */
    @Override
    public Mono<WriteResult> execute() {

        if (executed.compareAndSet(false, true)) {

            return Flux.merge(batchMonos)
                       .doOnNext(c -> c.forEach(batch::add))
                       .then(operations.getReactiveCqlOperations()
                                       .queryForResultSet(batch))
                       .flatMap(resultSet ->
                           resultSet.rows()
                                    .collectList()
                                    .map(rows -> new WriteResult(resultSet.getAllExecutionInfo(), resultSet.wasApplied(), rows))
                       );
        }

        throw new IllegalStateException("This Cassandra Batch was already executed");
    }

    /* (non-Javadoc)
     * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#withTimestamp(long)
     */
    @Override
    public ReactiveCassandraBatchOperations withTimestamp(long timestamp) {

        assertNotExecuted();

        batch.using(QueryBuilder.timestamp(timestamp));

        return this;
    }

    /* (non-Javadoc)
     * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#insert(java.lang.Object[])
     */
    @Override
    public ReactiveCassandraBatchOperations insert(Object... entities) {

        Assert.notNull(entities, "Entities must not be null");

        return insert(Arrays.asList(entities));
    }

    /* (non-Javadoc)
     * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#insert(java.lang.Iterable)
     */
    @Override
    public ReactiveCassandraBatchOperations insert(Iterable<?> entities) {
        return insert(entities, InsertOptions.empty());
    }

    /* (non-Javadoc)
     * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#insert(reactor.core.publisher.Mono)
     */
    @Override
    public ReactiveCassandraBatchOperations insert(Mono<? extends Iterable<?>> entities) {
        return insert(entities, InsertOptions.empty());
    }

    /* (non-Javadoc)
     * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#insert(java.lang.Iterable, org.springframework.data.cassandra.core.cql.WriteOptions)
     */
    @Override
    public ReactiveCassandraBatchOperations insert(Iterable<?> entities, WriteOptions options) {

        assertNotExecuted();
        Assert.notNull(entities, "Entities must not be null");
        Assert.notNull(options, "WriteOptions must not be null");

        batchMonos.add(Mono.just(doInsert(entities, options)));

        return this;
    }

    /* (non-Javadoc)
     * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#insert(reactor.core.publisher.Mono, org.springframework.data.cassandra.core.cql.WriteOptions)
     */
    @Override
    public ReactiveCassandraBatchOperations insert(Mono<? extends Iterable<?>> entities, WriteOptions options) {

        assertNotExecuted();
        Assert.notNull(entities, "Entities must not be null");
        Assert.notNull(options, "WriteOptions must not be null");

        batchMonos.add(entities.map(e -> doInsert(e, options)));

        return this;
    }

    Collection<? extends BuiltStatement> doInsert(Iterable<?> entities, WriteOptions options) {

        ArrayList<Insert> insertQueries = new ArrayList<>();
        CassandraMappingContext mappingContext = operations.getConverter().getMappingContext();

        for (Object entity : entities) {

            Assert.notNull(entity, "Entity must not be null");

            BasicCassandraPersistentEntity<?> persistentEntity = mappingContext
                    .getRequiredPersistentEntity(entity.getClass());
            insertQueries.add(QueryUtils.createInsertQuery(persistentEntity.getTableName().toCql(), entity, options,
                    operations.getConverter(), persistentEntity));
        }

        return insertQueries;
    }

    /* (non-Javadoc)
     * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#update(java.lang.Object[])
     */
    @Override
    public ReactiveCassandraBatchOperations update(Object... entities) {

        Assert.notNull(entities, "Entities must not be null");

        return update(Arrays.asList(entities));
    }

    /* (non-Javadoc)
     * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#update(java.lang.Iterable)
     */
    @Override
    public ReactiveCassandraBatchOperations update(Iterable<?> entities) {
        return update(entities, UpdateOptions.empty());
    }

    /* (non-Javadoc)
     * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#update(reactor.core.publisher.Mono)
     */
    @Override
    public ReactiveCassandraBatchOperations update(Mono<? extends Iterable<?>> entities) {
        return update(entities, UpdateOptions.empty());
    }

    /* (non-Javadoc)
     * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#update(java.lang.Iterable, org.springframework.data.cassandra.core.cql.WriteOptions)
     */
    @Override
    public ReactiveCassandraBatchOperations update(Iterable<?> entities, WriteOptions options) {

        assertNotExecuted();
        Assert.notNull(entities, "Entities must not be null");
        Assert.notNull(options, "WriteOptions must not be null");

        batchMonos.add(Mono.just(doUpdate(entities, options)));

        return this;
    }

    /* (non-Javadoc)
     * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#update(reactor.core.publisher.Mono, org.springframework.data.cassandra.core.cql.WriteOptions)
     */
    @Override
    public ReactiveCassandraBatchOperations update(Mono<? extends Iterable<?>> entities, WriteOptions options) {

        assertNotExecuted();
        Assert.notNull(entities, "Entities must not be null");
        Assert.notNull(options, "WriteOptions must not be null");

        batchMonos.add(entities.map(e -> doUpdate(e, options)));

        return this;
    }

    Collection<? extends BuiltStatement> doUpdate(Iterable<?> entities, WriteOptions options) {
        ArrayList<Update> updateQueries = new ArrayList<>();

        for (Object entity : entities) {

            Assert.notNull(entity, "Entity must not be null");

            updateQueries.add(QueryUtils.createUpdateQuery(getTable(entity), entity, options, operations.getConverter()));
        }

        return updateQueries;
    }

    /* (non-Javadoc)
     * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#delete(java.lang.Object[])
     */
    @Override
    public ReactiveCassandraBatchOperations delete(Object... entities) {

        Assert.notNull(entities, "Entities must not be null");

        return delete(Arrays.asList(entities));
    }

    /* (non-Javadoc)
     * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#delete(java.lang.Iterable)
     */
    @Override
    public ReactiveCassandraBatchOperations delete(Iterable<?> entities) {

        assertNotExecuted();
        Assert.notNull(entities, "Entities must not be null");

        batchMonos.add(Mono.just(doDelete(entities)));

        return this;
    }

    /* (non-Javadoc)
     * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#delete(reactor.core.publisher.Mono)
     */
    @Override
    public ReactiveCassandraBatchOperations delete(Mono<? extends Iterable<?>> entities) {

        assertNotExecuted();
        Assert.notNull(entities, "Entities must not be null");

        batchMonos.add(entities.map(this::doDelete));

        return this;
    }

    Collection<? extends BuiltStatement> doDelete(Iterable<?> entities) {
        ArrayList<Delete> deleteQueries = new ArrayList<>();

        for (Object entity : entities) {

            Assert.notNull(entity, "Entity must not be null");

            deleteQueries.add(QueryUtils.createDeleteQuery(getTable(entity), entity, QueryOptions.empty(), operations.getConverter()));
        }

        return deleteQueries;
    }

    private void assertNotExecuted() {
        Assert.state(!executed.get(), "This Cassandra Batch was already executed");
    }
    
    private String getTable(Object entity) {

        Assert.notNull(entity, "Entity must not be null");
        
        return operations.getConverter()
                         .getMappingContext()
                         .getRequiredPersistentEntity(ClassUtils.getUserClass(entity.getClass()))
                         .getTableName()
                         .toCql();
    }
}