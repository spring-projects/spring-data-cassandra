package org.springframework.data.cassandra.repository;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

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
    
    private final static String USER1_ID = "0001";
    private final static String USER2_ID = "0002";
    
    private User getUser1(){
        User user = new User();
        user.setUserId(USER1_ID);
        user.setFirstName("Brian");
        user.setLastName("Kelley");
        user.setCity("Philly");
        return user;
    }

    private User getUser2(){
        User user = new User();
        user.setUserId(USER2_ID);
        user.setFirstName("Lisa");
        user.setLastName("Collins");
        user.setCity("Owensville");
        return user;
    }

    @Test
    public void testSave() {   
        User user = this.getUser1();
        userRepository.save(user);
        assertTrue(userRepository.exists(USER1_ID));
    }

    @Test
    public void testFindOne() {   
        User user = this.getUser1();
        userRepository.save(user);
        assertTrue(userRepository.exists(USER1_ID));
        User findOne = userRepository.findOne(USER1_ID);
        assertNotNull(findOne);        
    }

    @Test
    public void testDeleteByID() throws InterruptedException {        
        User user = this.getUser1();
        userRepository.save(user);
        assertTrue(userRepository.exists(USER1_ID));
        userRepository.delete(USER1_ID);
        assertFalse(userRepository.exists(USER1_ID));        
    }

    @Test
    public void testDelete() throws InterruptedException {        
        User user = this.getUser1();
        userRepository.save(user);
        assertTrue(userRepository.exists(USER1_ID));
        userRepository.delete(USER1_ID);
        assertFalse(userRepository.exists(USER1_ID));        
    }
    
    @Test
    public void testDeleteSet() throws InterruptedException {        
        User user1 = this.getUser1();
        User user2 = this.getUser2();
        userRepository.save(user1);
        userRepository.save(user2);
        assertTrue(userRepository.exists(USER1_ID));
        assertTrue(userRepository.exists(USER2_ID));
        
        List<User> users = new ArrayList<User>();
        users.add(user1);
        users.add(user2);
        userRepository.delete(users);
        assertFalse(userRepository.exists(USER1_ID));        
        assertFalse(userRepository.exists(USER2_ID));        
    }

    @Test
    public void testCount() throws InterruptedException {        
        User user1 = this.getUser1();
        User user2 = this.getUser2();
        userRepository.save(user1);
        userRepository.save(user2);
        assertTrue(userRepository.exists(USER1_ID));
        assertTrue(userRepository.exists(USER2_ID));
        
        List<User> users = new ArrayList<User>();
        users.add(user1);
        users.add(user2);
        Assert.assertEquals(2, userRepository.count());
        
        userRepository.delete(users);
        assertFalse(userRepository.exists(USER1_ID));        
        assertFalse(userRepository.exists(USER2_ID));        
    }
    
}
