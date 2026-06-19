package com.kafkaops.security;

import java.util.Properties;

/**
 * CONCEPTUAL / TEMPLATE ONLY — not run against a live broker in this project.
 *
 * These methods build the Properties objects you'd use for SSL/TLS-secured
 * clients. They are correct, standard Kafka client configs — you can use
 * them directly once you have real certs/keystores — but main() here just
 * prints them rather than connecting, since enabling SSL requires actually
 * reconfiguring your broker with certificates first.
 *
 * Background:
 *  - Kafka's SSL/TLS encrypts data in transit between clients and brokers
 *    (and optionally broker-to-broker). It does NOT by itself authenticate
 *    *which user* is connecting beyond the cert identity — combine with
 *    SASL or mTLS client-cert auth for that (see SaslConfigTemplates).
 *  - Two-way TLS (mTLS) uses the client's own keystore so the broker can
 *    verify the client's identity from its certificate, not just encrypt
 *    the channel.
 */
public class SslConfigTemplates {

    /**
     * One-way TLS: client verifies the broker's certificate (via truststore),
     * broker does NOT verify the client. Encrypts traffic; doesn't authenticate
     * the client.
     */
    public static Properties oneWaySslClientConfig() {
        Properties props = new Properties();

        props.put("bootstrap.servers", "broker1:9093"); // SSL listener port, conventionally 9093 not 9092
        props.put("security.protocol", "SSL");

        // Truststore: the client's "list of CAs/certs I trust" so it can
        // verify the broker's certificate is legitimate.
        props.put("ssl.truststore.location", "/path/to/client.truststore.jks");
        props.put("ssl.truststore.password", "CHANGE_ME"); // use a secrets manager, never hardcode in real code

        // Optional: pin the broker hostname verification. Leaving this at
        // the default ("https") verifies the cert's CN/SAN matches the
        // broker hostname — don't disable this in production, it defeats
        // the point of TLS (prevents MITM).
        props.put("ssl.endpoint.identification.algorithm", "https");

        return props;
    }

    /**
     * Two-way TLS (mTLS): broker also verifies the client's certificate.
     * Adds a keystore (the client's own cert + private key) on top of the
     * one-way config above.
     */
    public static Properties mutualTlsClientConfig() {
        Properties props = oneWaySslClientConfig();

        // Keystore: this client's own identity (cert + private key) that
        // the broker will verify against its own truststore.
        props.put("ssl.keystore.location", "/path/to/client.keystore.jks");
        props.put("ssl.keystore.password", "CHANGE_ME");
        props.put("ssl.key.password", "CHANGE_ME"); // password for the private key entry itself

        return props;
    }

    /**
     * Broker-side equivalent (for reference — this goes in server.properties,
     * not a Java client). Shown here so the client config above makes sense
     * in context.
     */
    public static String brokerSideExampleConfig() {
        return """
                # server.properties excerpt (REFERENCE ONLY - not applied by this project)
                listeners=SSL://0.0.0.0:9093
                advertised.listeners=SSL://broker1.example.com:9093

                ssl.keystore.location=/path/to/broker.keystore.jks
                ssl.keystore.password=CHANGE_ME
                ssl.key.password=CHANGE_ME
                ssl.truststore.location=/path/to/broker.truststore.jks
                ssl.truststore.password=CHANGE_ME

                # Require client certs (mTLS) - omit for one-way TLS
                ssl.client.auth=required
                """;
    }

    public static void main(String[] args) {
        System.out.println("=== One-way TLS client config (template) ===");
        oneWaySslClientConfig().forEach((k, v) -> System.out.println(k + " = " + v));

        System.out.println("\n=== Mutual TLS (mTLS) client config (template) ===");
        mutualTlsClientConfig().forEach((k, v) -> System.out.println(k + " = " + v));

        System.out.println("\n=== Broker-side reference (server.properties) ===");
        System.out.println(brokerSideExampleConfig());

        System.out.println("These are templates only - swap in real keystore/truststore paths and");
        System.out.println("passwords (from a secrets manager, not hardcoded) before using against a real broker.");
    }
}
