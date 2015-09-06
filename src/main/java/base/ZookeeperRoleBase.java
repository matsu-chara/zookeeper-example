package base;

import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class ZookeeperRoleBase {
    protected final String serverId = Integer.toHexString((new Random()).nextInt());
    protected final Logger    Log;
    protected final ZooKeeper zk;

    public ZookeeperRoleBase(ZooKeeper zk) {
        Log = LoggerFactory.getLogger(this.getClass());
        this.zk = zk;
    }

    // InterruptedExceptionが検査例外であるため、lambdaの中で直接Thread.sleepを使うことができない。
    // 対策のためここで非検査例外に変換。
    protected void sleep(int sec) {
        try {
            Thread.sleep(sec * 1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
