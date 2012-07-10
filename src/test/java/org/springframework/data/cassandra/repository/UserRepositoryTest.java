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

    private User getUser1(){
        User user = new User();
        user.setUserId("0001");
        user.setFirstName("Brian");
        user.setLastName("ONeill");
        user.setCity("Philly");
        return user;
    }
    
    @Test
    public void testSave() {        
        userRepository.save(this.getUser1());
    }

    @Test
    public void testDelete() {        
        userRepository.save(this.getUser1());
    }

}
