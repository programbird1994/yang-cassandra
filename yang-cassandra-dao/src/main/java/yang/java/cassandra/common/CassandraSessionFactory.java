package yang.java.cassandra.common;

import com.datastax.driver.core.Session;

public interface CassandraSessionFactory {
    Session getSession();
    void close();
}
