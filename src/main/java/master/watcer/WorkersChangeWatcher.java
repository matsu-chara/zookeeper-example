package master.watcer;

import master.roles.WorkerController;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class WorkersChangeWatcher implements Watcher {
    WorkerController assigner;

    public WorkersChangeWatcher(WorkerController assigner) {
        this.assigner = assigner;
    }

    @Override public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.NodeChildrenChanged) {
            assert "/workers".equals(event.getPath());
            assigner.getWorkers();
        }
    }
}
