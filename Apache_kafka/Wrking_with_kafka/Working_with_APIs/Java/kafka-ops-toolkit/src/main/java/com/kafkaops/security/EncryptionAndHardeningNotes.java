package com.kafkaops.security;

/**
 * CONCEPTUAL REFERENCE — encryption at rest and general secure-configuration
 * practices. Kafka itself doesn't natively encrypt log segments on disk;
 * "at rest" encryption is normally handled at the infrastructure layer.
 */
public class EncryptionAndHardeningNotes {

    public static void main(String[] args) {
        System.out.println(encryptionAtRestNotes());
        System.out.println(secureConfigChecklist());
    }

    private static String encryptionAtRestNotes() {
        return """
                === Encryption at rest ===

                Kafka brokers write log segments to disk in plaintext by default —
                there's no built-in "encrypt the data files" feature. Options, in
                order of how most production setups actually do it:

                1. Disk/volume-level encryption (most common): encrypted EBS volumes
                   (AWS), encrypted persistent disks (GCP), Azure Disk Encryption, or
                   LUKS for self-managed hosts. Transparent to Kafka, zero app-level
                   changes, protects against stolen/discarded disks.

                2. Filesystem-level encryption (e.g. encrypted ZFS/dm-crypt mount for
                   the log.dirs path specifically) - useful if you want encryption
                   scoped to just the Kafka data directory rather than the whole volume.

                3. Application-level / client-side encryption: encrypt the message
                   payload before producing (e.g. envelope encryption with a KMS key),
                   decrypt after consuming. This is the only option that also protects
                   data from anyone with broker filesystem access (including other
                   tenants on a shared/managed cluster) - but it means brokers can't
                   inspect payloads (no compacted-topic dedup logic on content, etc.),
                   and you own key management and rotation.

                Most teams use #1 by default and add #3 only for specific
                highly-sensitive fields/topics (PII, payment data) where defense in
                depth against broker-level compromise actually matters.
                """;
    }

    private static String secureConfigChecklist() {
        return """
                === Secure configuration practices checklist ===

                - Disable PLAINTEXT listeners in production; SASL_SSL or SSL only.
                - Set ssl.endpoint.identification.algorithm=https (don't disable
                  hostname verification - it's what stops a MITM with a valid-but-wrong cert).
                - Use SCRAM or OAUTHBEARER over PLAIN where possible - avoid static
                  plaintext-transmitted passwords.
                - Principle of least privilege on ACLs: grant WRITE only to the
                  specific topics a service produces to, READ only to what it
                  consumes - avoid wildcard/ALL grants except for break-glass admin
                  accounts.
                - Rotate credentials and certs on a schedule, not just on suspected
                  compromise - and make sure the rotation process is actually tested,
                  not just documented.
                - Keep super.users (principals that bypass ACLs entirely) to an
                  absolute minimum - every super.user is a standing bypass of your
                  whole authorization model.
                - Audit: enable authorizer logging (authorizer.logger in
                  log4j.properties) so denied/allowed access attempts are traceable
                  during an incident.
                - Patch cadence: track CVEs for your Kafka version - auth/SSL bugs
                  are exactly the kind of thing that gets backported fixes you want
                  promptly.
                """;
    }
}
