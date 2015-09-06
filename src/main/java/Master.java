import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class Master implements Watcher {
    ZooKeeper zk;
    String hostPort;

    public Master(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZk() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    @Override public void process(WatchedEvent event) {
        System.out.println(event);
    }

    void stopZK() throws Exception { zk.close(); }

    public static void main(String args[]) throws Exception {
        Master m = new Master(args[0]);
        m.startZk();

        Thread.sleep(60000);
        m.stopZK();
    }
}
