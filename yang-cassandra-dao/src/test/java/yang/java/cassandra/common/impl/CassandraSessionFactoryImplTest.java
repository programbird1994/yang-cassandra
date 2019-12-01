package yang.java.cassandra.common.impl;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import yang.java.cassandra.common.CassandraClusterInitializer;
import yang.java.cassandra.common.CassandraSessionFactory;

@RunWith(value = JUnit4.class)
public class CassandraSessionFactoryImplTest {

    @Test
    public  void getSession() {
        String clusterName = "Yang Cluster";
        String contactPointsStr = "127.0.0.1:9043";
        String keySpace = "data";
        CassandraClusterInitializer cassandraClusterInitializer = new CassandraClusterInitializer(clusterName, contactPointsStr);
        CassandraSessionFactory cassandraSessionFactory = new CassandraSessionFactoryImpl(cassandraClusterInitializer, keySpace);
        Session session = cassandraSessionFactory.getSession();
        Assert.assertNotNull(session);
        ResultSet resultSet = session.execute("select * from data.user");
        resultSet.all().stream().forEach(System.out::println);
    }
}