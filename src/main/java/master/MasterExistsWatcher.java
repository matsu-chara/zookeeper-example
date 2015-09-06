package master;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class MasterExistsWatcher implements Watcher {
    Master master;

    public MasterExistsWatcher(Master master) {
        this.master = master;
    }

    @Override public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.NodeDeleted) {
            assert "/master".equals(event.getPath());
            master.runForMaster();
        }
    }
}
