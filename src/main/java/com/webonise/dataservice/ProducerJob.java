package com.webonise.dataservice;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import com.webonise.redis.model.User;

public class ProducerJob {
    private static final Logger LOG = LoggerFactory.getLogger(ProducerJob.class);
    
    @Value("${dir.basePath}")
    private String serDir;
    
    public void initJob() {
        LOG.info("ProducerJob is initialized by initJob");
    }
    
    public void produce() {
        try {
            LOG.info("ProducerJob runs at {}", new Date());
            List<User> users = generateUsers();
            deserializeUsers(users);
        } catch (Exception e) {
           LOG.error("Error occured while producing DTOs");
        }
    }

    private void deserializeUsers(List<User> users) {
        for ( User user : users ) {
            deserializeUser(user);
        }
        
    }

    private void deserializeUser(User user) {
        File file = new File(serDir, user.getId() + ".ser");
        try {
            FileOutputStream fos = new FileOutputStream(file);
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            SerializationUtils.serialize(user, oos);
            LOG.info("User ID: {}, NAME: {} is serialized", user.getId(), user.getName());
        } catch (Exception e) {
           LOG.error("Error occured while serializing User: {}", user.getId());
        }
        
        
    }

    private List<User> generateUsers() {
        SimpleDateFormat formatter = new SimpleDateFormat("hhmmss-");
        String timeStamp = formatter.format(new Date());
        List<User> userList = new ArrayList<User>();
        
        for ( int i = 0; i < 10; i++ ) {
            User user = new User("U-" + timeStamp + i, "USER-" + timeStamp + i);
            userList.add(user);
        }
        return userList;
    }
    
}
