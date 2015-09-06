package base;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.function.Consumer;

public class ZookeeperExecutor {
    private final String hostPort = MyZooKeeperConst.hostPort;
    private ZooKeeper zk;

    private void startZk(Watcher observer) throws IOException {
        zk = new ZooKeeper(hostPort, 15000, observer);
    }

    private void stopZK() throws InterruptedException {
        zk.close();
    }

    public void withZk(Watcher observer, Consumer<ZooKeeper> zkAction) throws IOException, InterruptedException {
        startZk(observer);
        zkAction.accept(zk);
        stopZK();
    }
}
