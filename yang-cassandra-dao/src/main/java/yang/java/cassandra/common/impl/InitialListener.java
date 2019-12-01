package yang.java.cassandra.common.impl;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;

public class InitialListener implements Host.StateListener {
    @Override
    public void onAdd(Host host) {

    }

    @Override
    public void onUp(Host host) {

    }


    @Override
    public void onDown(Host host) {

    }

    @Override
    public void onRemove(Host host) {

    }

    @Override
    public void onRegister(Cluster cluster) {

    }

    @Override
    public void onUnregister(Cluster cluster) {

    }
}
