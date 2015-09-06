package admin;

import base.PrintWatcher;
import base.ZookeeperExecutor;
import base.ZookeeperRoleBase;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException.*;

import java.util.Date;

class AdminClient extends ZookeeperRoleBase {

    public AdminClient(ZooKeeper zk) {
        super(zk);
    }

    public static void main(String[] args) throws Exception {
        ZookeeperExecutor exec    = new ZookeeperExecutor();
        PrintWatcher      watcher = new PrintWatcher();

        exec.withZk(watcher, zk -> {
            AdminClient ac = new AdminClient(zk);
            ac.listState();
        });
    }

    void listState() {
        try {
            listMaster();
            listWorkers();
            listTasks();
        } catch (KeeperException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void listMaster() throws KeeperException, InterruptedException {
        try {
            Stat stat = new Stat();
            byte masterData[] = zk.getData("/master", false, stat);
            Date startDate = new Date(stat.getCtime());
            System.out.println("Master: " + new String(masterData) + " since " + startDate);
        } catch (NoNodeException e) {
            System.out.println("No SyncMaster");
        }
    }

    private void listWorkers() throws KeeperException, InterruptedException {
        System.out.println("Worker:");
        for (String w : zk.getChildren("/workers", false)) {
            byte data[] = zk.getData("/workers/" + w, false, null);
            String state = new String(data);
            System.out.println("\t" + w + ": " + state);
        }
    }

    private void listTasks() throws KeeperException, InterruptedException {
        System.out.println("Tasks:");
        for (String t : zk.getChildren("/tasks", false)) {
            System.out.println("\t" + t);
        }
    }
}
