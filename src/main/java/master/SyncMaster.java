package master;

import base.PrintWatcher;
import base.ZookeeperExecutor;
import base.ZookeeperRoleBase;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

@Deprecated
class SyncMaster extends ZookeeperRoleBase {
    boolean isLeader = false;

    public SyncMaster(ZooKeeper zk) {
        super(zk);
    }

    public static void main(String args[]) throws Exception {
        ZookeeperExecutor       exec    = new ZookeeperExecutor();
        PrintWatcher watcher = new PrintWatcher();

        exec.withZk(watcher, zk -> {
            SyncMaster m = new SyncMaster(zk);
            m.runForMaster();
            if (m.isLeader) {
                System.out.println("I'm the leader");
                m.sleep(10);
            } else {
                System.out.println("Someone else is the leader");
            }
        });
    }

    void runForMaster() {
        while (true) {
            try {
                zk.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                isLeader = true;
                break;
            } catch (NoNodeException e) {
                isLeader = false;
                break;
            } catch (KeeperException ignored) {

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            if (checkMaster()) break;
        }
    }

    boolean checkMaster() {
        while (true) {
            try {
                Stat stat = new Stat();
                byte data[] = zk.getData("/master", false, stat);
                isLeader = new String(data).equals(serverId);
                return true;
            } catch (NoNodeException e) {
                return false;
            } catch (KeeperException ignored) {

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
