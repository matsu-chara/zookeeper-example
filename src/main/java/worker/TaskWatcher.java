package worker;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class TaskWatcher implements Watcher {
    Worker worker;
    String workerId;

    public TaskWatcher(Worker worker, String workerId) {
        this.worker = worker;
        this.workerId = workerId;
    }

    @Override public void process(WatchedEvent event) {
        if(event.getType() == Event.EventType.NodeChildrenChanged) {
            assert ("/assign/worker-" + workerId).equals(event.getPath());
            worker.getTasks();
        }
    }
}
