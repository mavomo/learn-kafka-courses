package io.confluent.developer.aggregate;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class TopicAclConfigurer {

   static public void createTopicWithAcls(Admin adminClient, List<NewTopic> topics, String serviceAccountPrincipal)
            throws Exception {
        // Create ACLs

        topics.forEach(
                topic -> {
                    List<AclBinding> aclBindings = Arrays.asList(
                            new AclBinding(
                                    new ResourcePattern(ResourceType.TOPIC, topic.name(), PatternType.LITERAL),
                                    new AccessControlEntry(
                                            serviceAccountPrincipal,
                                            "*",
                                            AclOperation.READ,
                                            AclPermissionType.ALLOW
                                    )
                            ),
                            new AclBinding(
                                    new ResourcePattern(ResourceType.TOPIC, topic.name(), PatternType.LITERAL),
                                    new AccessControlEntry(
                                            serviceAccountPrincipal,
                                            "*",
                                            AclOperation.WRITE,
                                            AclPermissionType.ALLOW
                                    )
                            )
                    );
                    // Create ACLs
                    try {
                        adminClient.createAcls(aclBindings).all().get();
                    } catch (InterruptedException e) {
                        System.out.println(" Error while creating ACLs - 1/2");
                        throw new RuntimeException(e);
                    } catch (ExecutionException e) {
                        System.out.println(" Error while creating ACLs  2/2");
                        throw new RuntimeException(e);
                    }
                }
        );




    }
}
