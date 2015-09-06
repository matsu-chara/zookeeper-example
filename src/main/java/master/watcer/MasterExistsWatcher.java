package master.watcer;

import master.roles.MasterElector;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class MasterExistsWatcher implements Watcher {
    MasterElector masterElector;

    public MasterExistsWatcher(MasterElector masterElector) {
        this.masterElector = masterElector;
    }

    @Override public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.NodeDeleted) {
            assert "/master".equals(event.getPath());
            masterElector.runForMaster();
        }
    }
}
