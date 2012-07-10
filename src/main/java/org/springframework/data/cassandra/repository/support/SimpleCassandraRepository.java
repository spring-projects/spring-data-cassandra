package org.springframework.data.cassandra.repository.support;

import java.io.Serializable;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;

import org.apache.commons.lang.NotImplementedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

/**
 * Repository base implementation for Cassandra.
 * 
 * @author Brian O'Neill
 */
public class SimpleCassandraRepository<T, ID extends Serializable> implements CassandraRepository<T, ID> {

    @Autowired
    EntityManagerFactory entityManagerFactory;

    /*
     * (non-Javadoc)
     * 
     * @see org.springframework.data.repository.CrudRepository#findOne(java.io.
     * Serializable)
     */
    public T findOne(ID id) {
        throw new NotImplementedException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.springframework.data.repository.CrudRepository#exists(java.io.
     * Serializable)
     */
    public boolean exists(ID id) {
        throw new NotImplementedException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.springframework.data.repository.CrudRepository#count()
     */
    public long count() {
        throw new NotImplementedException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.springframework.data.repository.CrudRepository#delete(java.io.
     * Serializable)
     */
    public void delete(ID id) {
        throw new NotImplementedException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.data.repository.CrudRepository#delete(java.lang.Object
     * )
     */
    public void delete(T entity) {
        throw new NotImplementedException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.data.repository.CrudRepository#delete(java.lang.Iterable
     * )
     */
    public void delete(Iterable<? extends T> entities) {
        throw new NotImplementedException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.springframework.data.repository.CrudRepository#deleteAll()
     */
    public void deleteAll() {
        throw new NotImplementedException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.springframework.data.repository.CrudRepository#findAll()
     */
    public List<T> findAll() {
        throw new NotImplementedException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.data.repository.CrudRepository#findAll(java.lang.
     * Iterable)
     */
    public Iterable<T> findAll(Iterable<ID> ids) {
        throw new NotImplementedException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.data.repository.PagingAndSortingRepository#findAll
     * (org.springframework.data.domain.Pageable)
     */
    public Page<T> findAll(final Pageable pageable) {
        throw new NotImplementedException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.data.repository.PagingAndSortingRepository#findAll
     * (org.springframework.data.domain.Sort)
     */
    public List<T> findAll(final Sort sort) {
        throw new NotImplementedException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.data.repository.CrudRepository#save(java.lang.Object)
     */
    public T save(T arg0) {
        EntityManager em = entityManagerFactory.createEntityManager();
        em.persist(arg0);
        em.close();
        return arg0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.data.repository.CrudRepository#save(java.lang.Iterable
     * )
     */
    public Iterable<T> save(Iterable<? extends T> arg0) {
        throw new NotImplementedException();
    }

}
