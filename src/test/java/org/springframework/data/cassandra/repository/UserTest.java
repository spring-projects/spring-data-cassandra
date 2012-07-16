package org.springframework.data.cassandra.repository;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.Test;
import org.junit.Ignore;
import org.springframework.data.cassandra.repository.example.User;

@Ignore
public class UserTest {
    
    @Test
    public void testWrite() {
        User user = new User();
        user.setUserId("0001");
        user.setFirstName("John");
        user.setLastName("Smith");
        user.setCity("London");

        EntityManagerFactory emf = Persistence.createEntityManagerFactory("cassandra_pu");
        EntityManager em = emf.createEntityManager();

        em.persist(user);
        em.close();
        emf.close();
    }
    
}
