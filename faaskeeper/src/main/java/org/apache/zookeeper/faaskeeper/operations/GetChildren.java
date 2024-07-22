package org.apache.zookeeper.faaskeeper.operations;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.faaskeeper.queue.CloudDirectResult;

public class GetChildren extends DirectOperation {

    public GetChildren(String sessionID, String path, Watcher watcher) {
        super(sessionID, path, watcher);
    }

    public String getName() {
        return "get_children";
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
                if (this.cb instanceof ChildrenCallback) {
                    ((AsyncCallback.ChildrenCallback)this.cb).processResult(rc.intValue(), this.getPath(), this.callbackCtx, null);
                } else if (this.cb instanceof Children2Callback) {
                    ((AsyncCallback.Children2Callback)this.cb).processResult(rc.intValue(), this.getPath(), this.callbackCtx, null, null);
                }
            }
        } else {
            event.future.complete(event.result);

            if (this.cb != null) {
                GetChildrenResult res = (GetChildrenResult) event.result;
                rc = Code.OK;

                if (this.cb instanceof ChildrenCallback) {
                    ((AsyncCallback.ChildrenCallback)this.cb).processResult(rc.intValue(), this.getPath(), this.callbackCtx, res.getChildren());
                } else if (this.cb instanceof Children2Callback) {
                    ((AsyncCallback.Children2Callback)this.cb).processResult(rc.intValue(), this.getPath(), this.callbackCtx, res.getChildren(), res.getStat());
                }
            }
        }
    }
}
