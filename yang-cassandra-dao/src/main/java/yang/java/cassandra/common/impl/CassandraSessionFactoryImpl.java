package yang.java.cassandra.common.impl;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import yang.java.cassandra.common.CassandraSessionFactory;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CassandraSessionFactoryImpl implements CassandraSessionFactory {
    private Cluster cluster;
    private Lock lock = new ReentrantLock();
    private Session session;
    private String keySpace;

    public CassandraSessionFactoryImpl(Cluster.Initializer initializer, String keySpace) {
        this.cluster = Cluster.buildFrom(initializer)
        ;
        this.keySpace = keySpace;
    }

    public Session getSession() {
        if (session == null) {
            lock.tryLock();
            try {
                if (session == null) {
                    session = cluster.connect(keySpace);
                }
            } finally {
                lock.unlock();
            }
        }

        return session;
    }

    public void close() {
        if (!session.isClosed()) {
            session.close();
        }
        if (cluster.isClosed()) {
            cluster.close();
        }
    }
}
