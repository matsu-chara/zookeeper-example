package chapter3.client;

import chapter3.base.PrintWatcher;
import chapter3.base.ZookeeperExecutor;
import chapter3.base.ZookeeperRoleBase;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.*;

import java.util.UUID;

class Client extends ZookeeperRoleBase {

    public Client(ZooKeeper zk) {
        super(zk);
    }

    public static void main(String[] args) throws Exception {
        ZookeeperExecutor exec    = new ZookeeperExecutor();
        PrintWatcher      watcher = new PrintWatcher();

        exec.withZk(watcher, zk -> {
            Client c = new Client(zk);
            c.queueCommand(UUID.randomUUID().toString());
        });

    }

    String queueCommand(String command) {
        while (true) {
            try {
                return zk.create("/tasks/task-",
                                        command.getBytes(),
                                        Ids.OPEN_ACL_UNSAFE,
                                        CreateMode.PERSISTENT_SEQUENTIAL);
            } catch (NodeExistsException e) {
                throw new RuntimeException(e.getPath() + " already appears to be running");
            } catch (ConnectionLossException ignored) {

            } catch (KeeperException | InterruptedException e) {
                throw new RuntimeException(e);
            }

        }
    }
}
