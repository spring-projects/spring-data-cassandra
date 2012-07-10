package org.springframework.data.cassandra.repository;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Ignore;
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
    
    private final static String USER_ID = "0002";
    
    private User getUser1(){
        User user = new User();
        user.setUserId(USER_ID);
        user.setFirstName("Brian");
        user.setLastName("Kelley");
        user.setCity("Philly");
        return user;
    }
    
    @Test
    public void testSave() {   
        User user = this.getUser1();
        userRepository.save(user);
        assertTrue(userRepository.exists(user.getUserId()));
    }

    @Test
    public void testFindOne() {   
        User user = this.getUser1();
        userRepository.save(user);
        assert(userRepository.exists(user.getUserId()));
        User findOne = userRepository.findOne(user.getUserId());
        assertNotNull(findOne);        
    }

    @Test
    public void testDelete() throws InterruptedException {        
        userRepository.delete(USER_ID);
        assertFalse(userRepository.exists(USER_ID));        
    }

}
