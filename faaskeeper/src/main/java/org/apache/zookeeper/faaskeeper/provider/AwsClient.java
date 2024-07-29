package org.apache.zookeeper.faaskeeper.provider;
// ddb deps
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;

// sqs deps
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.math.BigInteger;
import java.util.stream.Collectors;


import org.apache.zookeeper.faaskeeper.model.Node;
import org.apache.zookeeper.faaskeeper.model.QueueType;
import org.apache.zookeeper.faaskeeper.model.Version;
import org.apache.zookeeper.faaskeeper.queue.CloudProviderException;
import org.apache.zookeeper.faaskeeper.model.EpochCounter;
import org.apache.zookeeper.faaskeeper.model.SystemCounter;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.faaskeeper.FaasKeeperConfig;

public class AwsClient extends ProviderClient {
    private final AmazonSQS sqs;
    private final DynamoDbClient ddb;
    private final String userTable;
    private final String dataTable;
    private static final Logger LOG;
    private final String writerQueueName;
    private final String writerQueueUrl;
    static {
        LOG = LoggerFactory.getLogger(AwsClient.class);
    }
    
    public AwsClient(FaasKeeperConfig config) {
        super(config);

        this.ddb = DynamoDbClient.builder()
            .region(Region.of(config.getDeploymentRegion()))
            .build();
        this.sqs = AmazonSQSClientBuilder.standard()
            .withRegion(config.getDeploymentRegion()) // Specify the desired AWS region
            .build();
        
        this.userTable = String.format("faaskeeper-%s-users", config.getDeploymentName());

        this.dataTable = String.format("faaskeeper-%s-data", config.getDeploymentName());

        this.writerQueueName = String.format("faaskeeper-%s-writer-sqs.fifo", config.getDeploymentName());
        // this.writerQueueName = "fk-test.fifo";
        this.writerQueueUrl = sqs.getQueueUrl(writerQueueName).getQueueUrl();
    }

    public void registerSession(String sessionId, String sourceAddr, boolean heartbeat) {
        Map<String, AttributeValue> itemValues = new HashMap<>();
        itemValues.put("user", AttributeValue.builder().s(sessionId).build());
        itemValues.put("source_addr", AttributeValue.builder().s(sourceAddr).build());
        itemValues.put("ephemerals", AttributeValue.builder().l(new ArrayList<>()).build());
        PutItemRequest request = PutItemRequest.builder()
            .tableName(userTable)
            .item(itemValues)
            .build();

        try {
            PutItemResponse response = ddb.putItem(request);
            LOG.info(userTable + " was successfully updated. The request id is " + response.responseMetadata().requestId());

        } catch (ResourceNotFoundException e) {
            LOG.error(String.format("Error: The Amazon DynamoDB table \"%s\" can't be found.\n", userTable));
        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
        }
    }

    public void sendRequest(String requestId, Map <String, Object> data) throws CloudProviderException {
        try {
            if (config.getWriterQueue() == QueueType.SQS) { 
                ObjectMapper objectMapper = new ObjectMapper();
                
                // Format data for ddb format and convert to JSON string
                String serializedData = objectMapper.writeValueAsString(DynamoReader.convertItems(data));
                // String serializedData = objectMapper.writeValueAsString(data);

                LOG.info("Attempting to send msg: " + serializedData);

                SendMessageRequest send_msg_request = new SendMessageRequest()
                    .withMessageGroupId("0")
                    .withMessageDeduplicationId(requestId)
                    .withQueueUrl(writerQueueUrl)
                    .withMessageBody(serializedData);
                
                SendMessageResult res = sqs.sendMessage(send_msg_request);
                LOG.debug("Msg sent successfully: " + res.toString());
            } else {
                throw new CloudProviderException("Unsupported queue type specified in sendRequest: " + config.getWriterQueue().name());
            }

        } catch(Exception e) {
            LOG.error("Error sending request: ", e);
            throw new CloudProviderException(e);
        }

    }

    public Node getData(String path) throws DynamoDbException, KeeperException {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("path", AttributeValue.builder().s(path).build());

        GetItemRequest getRequest = GetItemRequest.builder()
            .tableName(dataTable)
            .key(key)
            .consistentRead(true)
            .build();

        try {
            Map<String, AttributeValue> result = ddb.getItem(getRequest).item();

            if (result != null && !result.isEmpty()) {
                // TODO: Remove later
                // for (Map.Entry<String, AttributeValue> entry : result.entrySet()) {
                //     LOG.debug("Key: " + entry.getKey() + ", Value: " + entry.getValue().toString());
                // }

                Node node = new Node(path);

                List<BigInteger> modifiedSysCounter = result.containsKey("mFxidSys") && !result.get("mFxidSys").l().isEmpty()
                    ? result.get("mFxidSys").l().stream()
                        .map(AttributeValue::n)
                        .map(BigInteger::new)
                        .collect(Collectors.toList())
                    : null;
                                    
                Set<String> modifiedEpochCounter = result.containsKey("mFxidEpoch") && !result.get("mFxidEpoch").ss().isEmpty() ?
                    result.get("mFxidEpoch").ss().stream()
                        .filter(s -> !s.isEmpty())
                        .collect(Collectors.toSet()) : null;
                

                byte[] data = result.get("data") != null ? result.get("data").b().asByteArray() : null;

                List<String> children = result.containsKey("children") && !result.get("children").l().isEmpty() ? 
                        result.get("children").l().stream()
                            .map(AttributeValue::s)
                            .collect(Collectors.toList()) : null;

                List<BigInteger> createdSysCounter = result.containsKey("cFxidSys") && !result.get("cFxidSys").l().isEmpty()
                    ? result.get("cFxidSys").l().stream()
                        .map(AttributeValue::n)
                        .map(BigInteger::new)
                        .collect(Collectors.toList())
                    : null;    

                if (createdSysCounter != null) {
                    node.setCreated(new Version(SystemCounter.fromRawData(createdSysCounter), null));
                } else {
                    // Should an exception be raised here?
                    LOG.error("Fatal error: Node missing created system counter");
                }

                if (modifiedSysCounter != null) {
                    node.setModified(new Version(SystemCounter.fromRawData(modifiedSysCounter), 
                        modifiedEpochCounter != null ? EpochCounter.fromRawData(modifiedEpochCounter) : null));
                }

                if (children != null) {
                    node.setChildren(children);
                } else {
                    node.setChildren(new ArrayList<String>());
                }

                if (data != null) {
                    node.setData(data);
                    node.setDataB64(Base64.getEncoder().encodeToString(data));
                } else {
                    node.setData(new byte[]{});
                    node.setDataB64("");
                }
                
                return node;
            } else {
                LOG.debug("Empty result from ddb. Node does not exist");
                throw new NoNodeException(path);
            }
        } catch (DynamoDbException e) {
            LOG.error("Error fetching node data from DynamoDB: " + e.getMessage());
            throw e;
        }
    }



}
