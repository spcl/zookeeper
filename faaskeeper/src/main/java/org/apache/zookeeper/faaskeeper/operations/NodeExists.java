package org.apache.zookeeper.faaskeeper.operations;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.faaskeeper.FaasKeeperClient;
import org.apache.zookeeper.faaskeeper.model.Node;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.faaskeeper.queue.CloudDirectResult;

public class NodeExists extends DirectOperation {

    public NodeExists(String sessionID, String path, Watcher watcher) {
        super(sessionID, path, watcher);
    }

    public String getName() {
        return "exists";
    }

    public void processResult(CloudDirectResult event) {

        Code rc;

        if (event.result instanceof ReadExceptionResult) {
            ReadExceptionResult res = (ReadExceptionResult) event.result;
            event.future.completeExceptionally(res.getException());
            
            rc = Code.SYSTEMERROR;
            if (this.cb != null) {
                ((AsyncCallback.StatCallback)this.cb).processResult(rc.intValue(), this.getPath(), this.callbackCtx, null);
            }

        } else {
            event.future.complete(event.result);

            if (this.cb != null) {
                GetDataResult res = (GetDataResult) event.result;
                Node n = res.getNode().orElse(null);

                if (n != null) {
                    rc = Code.OK;
                    Stat stat = new Stat();
                    FaasKeeperClient.updateStat(stat, n, null);
                    ((AsyncCallback.StatCallback)this.cb).processResult(rc.intValue(), this.getPath(), this.callbackCtx, stat);

                } else {
                    rc = Code.NONODE;
                    ((AsyncCallback.StatCallback)this.cb).processResult(rc.intValue(), this.getPath(), this.callbackCtx, null);
                }
            }
        }


    }

}
