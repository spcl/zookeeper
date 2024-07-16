package org.apache.zookeeper;

import org.apache.zookeeper.data.Stat;

// snippet-start:[dynamodb.java2.put_item.main]
// snippet-start:[dynamodb.java2.put_item.import]
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.zookeeper.faaskeeper.model.Node;
import org.apache.zookeeper.faaskeeper.operations.GetDataResult;
import org.apache.zookeeper.faaskeeper.operations.ReadOpResult;
import org.apache.zookeeper.faaskeeper.thread.SorterThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.faaskeeper.FaasKeeperClient;


/**
 * Before running this Java V2 code example, set up your development environment, including your credentials.
 *
 * For more information, see the following documentation topic:
 *
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 *
 *  To place items into an Amazon DynamoDB table using the AWS SDK for Java V2,
 *  its better practice to use the
 *  Enhanced Client. See the EnhancedPutItem example.
 */
// {"user": session_id, "addr": source_addr, "ephemerals": []}
public class App {
    private static final Logger LOG;
    static {
        LOG = LoggerFactory.getLogger(App.class);
    }

    public static void main(String[] args) {
        try {
            // testFK();
            testZK();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static String getUUID() {
        return UUID.randomUUID().toString();
    }

    public static void testZK() throws Exception {
        String filePath = "/Users/syed/Desktop/gsoc/java-testbed/test-config.json";
        FaasKeeperClient client = FaasKeeperClient.buildClient(filePath, false);
        client.start();
        byte[] data = {1, 2};
        String nodePath = "/test-node-syed-dev-4";
        String randomPath = "/" + UUID.randomUUID().toString();
        
        AsyncCallback.Create2Callback cbc = new AsyncCallback.Create2Callback() {
            public void processResult(int rc, String path, Object ctx, String name, Stat stat) {
                switch (KeeperException.Code.get(rc)) {
                    case OK:
                        System.out.println("Znode created successfully: " + name);
                        System.out.println(stat.getDataLength());
                        break;
                    case NODEEXISTS:
                        System.out.println("Znode already exists: " + path);
                        break;
                    default:
                        System.out.println("Error occurred: " + KeeperException.Code.get(rc));
                        break;
                }
            }
        };

        AsyncCallback.StringCallback cbs = new AsyncCallback.StringCallback() {
            public void processResult(int rc, String path, Object ctx, String name) {
                    switch (KeeperException.Code.get(rc)) {
                        case OK:
                            System.out.println("Znode created successfully: " + name);
                            break;
                        case NODEEXISTS:
                            System.out.println("Znode already exists: " + path);
                            break;
                        default:
                            System.out.println("Error occurred: " + KeeperException.Code.get(rc));
                            break;
                    }
            }
        };



        try {
            // client.create(nodePath, data, null, CreateMode.PERSISTENT, cbs, null);
            client.create(nodePath, data, null, CreateMode.PERSISTENT, cbc, null);
            // String res = client.create("/" + getUUID(), data, null, CreateMode.PERSISTENT);
            // System.out.format("Created: {}", res);
            Thread.sleep(10000);
            System.out.println("Done");

        } catch (Exception e) {
            System.out.println("Error occurred during node creation: " + e.getMessage());
            e.printStackTrace();
        }
        client.stop();
        System.out.println("Stopped client, exiting program now");
    }

    public static void testFK() throws Exception {
        String filePath = "/Users/syed/Desktop/gsoc/java-testbed/test-config.json";
        FaasKeeperClient client = FaasKeeperClient.buildClient(filePath, false);
        client.start();
        byte[] data = {1, 2, 3, 4, 5, 6};
        String res = "";
        String nodePath = "/";//"/test-node-syed-dev-0";
        // try {
            // res = client.create(nodePath, data, 0);
            // System.out.println("Created: " + res);
        // } catch (Exception e) {
            // e.printStackTrace();
        // }
        Node n;

        try {
            // res = client.getDataSync("/test-node-syed-dev-0").getDataB64();
            // System.out.println("State-1: " + res);
            // n = client.setDataSync("/test-node-syed-dev-0", data, -1);
            // res = client.getDataSync("/test-node-syed-dev-0").getDataB64();
            // System.out.println("State-2: " + res);
            
            // CompletableFuture<GetDataResult> f = client.existsAsync("abcd");
            // n = f.get().getNode().orElse(null);
            // assert n == null;

            // n = client.existsSync("/test-node-syed-dev-0");
            // assert n != null;
            // System.out.println("DATAA: " + n.getDataB64());

            // List<String> l = client.getChildrenSync(nodePath);
            // for(String ch: l) {
            //     System.out.println("Child getchildop: " + ch);
            // }
            // CompletableFuture<Node> fut = client.createAsync("/test-node-syed-dev-22", data, 0);
            // Node n = fut.get();
            // System.out.println("Create done: " + n.getPath());
            // byte[] updat = {1, 1, 1, 1, 1};
            // Node n = client.setData(nodePath, updat, -1);
            
            // client.delete(nodePath, -1);


            // res = client.create("/test-node-syed-dev-22", data, 0);
            // System.out.println("Created: " + res);

            // for (BigInteger i: n.getModified().getSystem().serialize()) {
            //     System.out.println("Modified num: ");
            //     System.out.println(i);
            // }
            // f = client.getDataAsync(nodePath);
            // n = f.get().getNode().get();
            // for(String ch: n.getChildren()) {
            //     System.out.println("Child: " + ch);
            // }

        } catch (Exception e) {
            System.out.println("Error occurred during node creation: " + e.getMessage());
            e.printStackTrace();
        }
        client.stop();
        System.out.println("Stopped client, exiting program now");
        // String content = "";
        // try {
        //     content = new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(filePath)));
        // } catch (java.io.IOException e) {
        //     System.err.println("Failed to read file: " + e.getMessage());
        // }
        // System.out.println("File content: " + content);

        // try {
        //     FaasKeeperConfig cfg = FaasKeeperConfig.deserialize(content);
        //     System.out.println("Config: " + cfg.getProviderConfig().getDataBucket());
        // } catch (Exception e) {
        //     System.err.println("Failed to deserialize config: " + e.getMessage());
        // }
    }

    public static void testDDB() {
        String tableName = "faaskeeper-dev-0-users";
        String user = "42";
        String sourceAddress = "127.0.0.1";

        Region region = Region.US_EAST_1;
        DynamoDbClient ddb = DynamoDbClient.builder()
            .region(region)
            .build();

        add_session(ddb, tableName, "user", user, "source_addr", sourceAddress);
        testDeleteSession(ddb, tableName, "user", user);

        System.out.println("Done!");
        ddb.close();
    }

    public static void add_session(DynamoDbClient ddb,
                                      String tableName,
                                      String key,
                                      String keyVal,
                                      String sourceAddr,
                                      String sourceAddrVal) {

        HashMap<String, AttributeValue> itemValues = new HashMap<>();
        itemValues.put(key, AttributeValue.builder().s(keyVal).build());
        itemValues.put(sourceAddr, AttributeValue.builder().s(sourceAddrVal).build());
        itemValues.put("ephemerals", AttributeValue.builder().l(new ArrayList<>()).build());

        PutItemRequest request = PutItemRequest.builder()
            .tableName(tableName)
            .item(itemValues)
            .build();

        try {
            PutItemResponse response = ddb.putItem(request);
            System.out.println(tableName + " was successfully updated. The request id is " + response.responseMetadata().requestId());

        } catch (ResourceNotFoundException e) {
            System.err.format("Error: The Amazon DynamoDB table \"%s\" can't be found.\n", tableName);
            System.err.println("Be sure that it exists and that you've typed its name correctly!");
            System.exit(1);
        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    public static void testDeleteSession(DynamoDbClient ddb, String tableName, String key, String keyVal) {
        HashMap<String, AttributeValue> keyToGet = new HashMap<>();
        keyToGet.put(key, AttributeValue.builder()
            .s(keyVal)
            .build());

        DeleteItemRequest deleteReq = DeleteItemRequest.builder()
            .tableName(tableName)
            .key(keyToGet)
            .build();

        try {
            ddb.deleteItem(deleteReq);
        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }
}
