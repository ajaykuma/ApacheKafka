package com.kafkaops.security;

/**
 * CONCEPTUAL REFERENCE — ACLs are normally managed via the kafka-acls.sh CLI
 * tool (or AdminClient's createAcls()/describeAcls() if you wanted to
 * automate this in Java — shown below as commented reference code, not
 * executed, since it would actually mutate ACLs on a real broker).
 *
 * Authorization model:
 *   Principal (who) + Operation (what) + Resource (on what) + Host (from where)
 *
 * Common operations: READ, WRITE, CREATE, DELETE, ALTER, DESCRIBE,
 * CLUSTER_ACTION, IDEMPOTENT_WRITE, ALL
 *
 * Common resource types: TOPIC, GROUP, CLUSTER, TRANSACTIONAL_ID
 */
public class AclReference {

    public static void main(String[] args) {
        System.out.println(cliExamples());
        System.out.println(roleBasedAccessNotes());
    }

    private static String cliExamples() {
        return """
                === kafka-acls.sh examples (run on a broker host, not from this project) ===

                # Allow a producer principal to write to one topic
                kafka-acls.sh --bootstrap-server broker1:9092 \\
                  --add --allow-principal User:order-service \\
                  --operation WRITE --topic orders

                # Allow a consumer principal to read a topic AND use its consumer group
                kafka-acls.sh --bootstrap-server broker1:9092 \\
                  --add --allow-principal User:order-processor \\
                  --operation READ --topic orders \\
                  --operation READ --group order-processor-group

                # Deny a principal explicitly (deny rules take precedence over allow)
                kafka-acls.sh --bootstrap-server broker1:9092 \\
                  --add --deny-principal User:contractor-temp \\
                  --operation WRITE --topic orders

                # List current ACLs for a topic
                kafka-acls.sh --bootstrap-server broker1:9092 --list --topic orders

                # Wildcard / prefixed resource pattern (e.g. all topics starting with "team-a-")
                kafka-acls.sh --bootstrap-server broker1:9092 \\
                  --add --allow-principal User:team-a-service \\
                  --operation ALL --topic team-a- --resource-pattern-type prefixed
                """;
    }

    private static String roleBasedAccessNotes() {
        return """
                === Role-based access concepts (RBAC layered on top of ACLs) ===

                Plain Kafka ACLs are principal-to-resource grants — there's no built-in
                "role" concept (group of permissions you assign to many principals at
                once). In practice, teams build RBAC one of these ways:

                1. Naming convention + automation: define roles (e.g. "topic-producer",
                   "topic-consumer", "topic-admin") as a fixed set of ACL templates in
                   your provisioning tool (Terraform, GitOps), and apply the template
                   per-principal-per-topic rather than managing ACLs by hand.

                2. Commercial/managed platforms (Confluent RBAC, Conduktor, etc.) bolt
                   a real role layer on top of Kafka's ACL or a custom authorizer,
                   letting you assign roles like "DeveloperRead" or "ResourceOwner" to
                   users/groups directly.

                3. KRaft/ZooKeeper-independent custom Authorizer implementation: Kafka's
                   `authorizer.class.name` is pluggable — larger orgs sometimes write a
                   custom authorizer that checks an external policy/role service
                   (e.g. OPA - Open Policy Agent) instead of static ACLs.

                For a small-to-mid setup, option 1 (naming convention + automation) is
                usually the pragmatic choice: it gets you role-like consistency without
                needing a commercial platform or custom authorizer code.
                """;
    }
}
