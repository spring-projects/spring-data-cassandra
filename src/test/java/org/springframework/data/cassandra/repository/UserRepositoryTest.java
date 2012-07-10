package org.springframework.data.cassandra.repository;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.repository.example.User;
import org.springframework.data.cassandra.repository.example.UserRepository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:beans.xml")
public class UserRepositoryTest {
    @Autowired
    UserRepository userRepository;
    
    @Test
    public void testWrite() {
        User user = new User();
        user.setUserId("0001");
        user.setFirstName("John");
        user.setLastName("Smith");
        user.setCity("London");        
        userRepository.save(user);
    }

}
