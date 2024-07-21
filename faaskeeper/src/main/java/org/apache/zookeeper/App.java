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

import java.util.concurrent.CountDownLatch;
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
            String filePath = "/Users/syed/Desktop/gsoc/java-testbed/test-config.json";
            FaasKeeperClient client = FaasKeeperClient.buildClient(filePath, false, null);
            client.start();
            String newNode = "/test-node-1";

            testZKCreate(client, newNode);
            testZKExists(client, newNode);
            testZKExists(client, "/");
            testZKDelete(client, newNode);
            
            client.stop();
        } catch (Exception e) {
            System.out.println(e);
            e.printStackTrace();
        }
    }

    public static String getUUID() {
        return UUID.randomUUID().toString();
    }
    
    public static void testZKExists(FaasKeeperClient client, String path) throws Exception {
        client.exists(path, false, new AsyncCallback.StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                // TODO Auto-generated method stub
                LOG.info("Exists returned: " + String.valueOf(rc) + " for " + path);
                if (stat != null) {
                    System.out.format("Numchild: %d\n", stat.getNumChildren());
                }
            }   
        }, null);
        Thread.sleep(10000);
    }

    public static void testZKDelete(FaasKeeperClient client, String nodeToDelete) throws Exception {
        client.delete(nodeToDelete, -1, new AsyncCallback.VoidCallback() {
            public void processResult(int rc, String path, Object ctx) {
                System.out.println("Delete returned: " + String.valueOf(rc));
            }
        }, null);
        Thread.sleep(10000);
    }

    public static void testZKCreate(FaasKeeperClient client, String nodePath) throws Exception {
        byte[] data = {1, 2};
        // String randomPath = "/" + UUID.randomUUID().toString();
        
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

        // client.create(nodePath, data, null, CreateMode.PERSISTENT, cbs, null);
        client.create(nodePath, data, null, CreateMode.PERSISTENT, cbc, null);
        // String res = client.create("/" + getUUID(), data, null, CreateMode.PERSISTENT);
        // System.out.format("Created: {}", res);
        Thread.sleep(10000);
    }

    public static void testDDB() {
        String tableName = "faaskeeper-dev-0-users";
        String user = "42";
        String sourceAddress = "127.0.0.1";

        Region region = Region.US_EAST_1;
        DynamoDbClient ddb = DynamoDbClient.builder()
            .region(region)
            .build();

        testAddSession(ddb, tableName, "user", user, "source_addr", sourceAddress);
        testDeleteSession(ddb, tableName, "user", user);

        System.out.println("Done!");
        ddb.close();
    }

    public static void testAddSession(DynamoDbClient ddb,
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
