package org.apache.zookeeper.faaskeeper.operations;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.faaskeeper.FaasKeeperClient;
import org.apache.zookeeper.faaskeeper.model.Node;
import org.apache.zookeeper.faaskeeper.queue.CloudDirectResult;

public class GetData extends DirectOperation {

    public GetData(String sessionID, String path, Watcher watcher) {
        super(sessionID, path, watcher);
    }

    public String getName() {
        return "get_data";
    }

    public void processResult(CloudDirectResult event) {

        Code rc;

        if (event.result instanceof ReadExceptionResult) {
            ReadExceptionResult res = (ReadExceptionResult) event.result;
            event.future.completeExceptionally(res.getException());
            
            rc = Code.SYSTEMERROR;
            if (res.getException() instanceof NoNodeException) {
                rc = Code.NONODE;
            }

            if (this.cb != null) {
                ((AsyncCallback.DataCallback)this.cb).processResult(rc.intValue(), this.getPath(), this.callbackCtx, null, null);
            }
        } else {
            event.future.complete(event.result);

            if (this.cb != null) {
                GetDataResult res = (GetDataResult) event.result;
                Node n = res.getNode().get();
                rc = Code.OK;
                Stat stat = new Stat();
                
                FaasKeeperClient.updateStat(stat, n, null);
                ((AsyncCallback.DataCallback)this.cb).processResult(rc.intValue(), this.getPath(), this.callbackCtx, n.getData(), stat);
            }
        }
    }
}
