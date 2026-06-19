package com.kafkaops.security;

import java.util.Properties;

/**
 * CONCEPTUAL / TEMPLATE ONLY — not run against a live broker in this project.
 *
 * SASL is Kafka's pluggable *authentication* layer — "who is this client".
 * It's normally combined with SSL/TLS for encryption (SASL_SSL), since SASL
 * alone (SASL_PLAINTEXT) authenticates but doesn't encrypt — credentials
 * would go over the wire in the clear with PLAIN mechanism otherwise.
 */
public class SaslConfigTemplates {

    /**
     * SASL/PLAIN: simplest mechanism, username+password checked against a
     * JAAS config (often backed by a simple file or an external store via a
     * custom callback handler). Must be paired with SSL in any real
     * deployment, since PLAIN sends the password in the clear within the
     * SASL exchange itself.
     */
    public static Properties saslPlainConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "broker1:9094"); // conventional SASL_SSL port
        props.put("security.protocol", "SASL_SSL"); // SASL for auth + SSL for encryption
        props.put("sasl.mechanism", "PLAIN");

        // JAAS config inline (alternative: external jaas.conf file + java.security.auth.login.config)
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"CHANGE_ME\" " +
                "password=\"CHANGE_ME\";"); // pull from secrets manager, never hardcode in real code

        // Still need the truststore for the SSL half of SASL_SSL
        props.put("ssl.truststore.location", "/path/to/client.truststore.jks");
        props.put("ssl.truststore.password", "CHANGE_ME");

        return props;
    }

    /**
     * SASL/SCRAM (SCRAM-SHA-256 or SCRAM-SHA-512): like PLAIN but the
     * password is never sent over the wire — a salted challenge-response
     * exchange proves the client knows the password without transmitting
     * it. Credentials are stored in ZooKeeper/KRaft, managed via
     * kafka-configs.sh --alter --add-config, not a flat file. Preferred
     * over PLAIN when you don't want plaintext credential files at all.
     */
    public static Properties saslScramConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "broker1:9094");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "SCRAM-SHA-512"); // or SCRAM-SHA-256

        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                "username=\"CHANGE_ME\" " +
                "password=\"CHANGE_ME\";");

        props.put("ssl.truststore.location", "/path/to/client.truststore.jks");
        props.put("ssl.truststore.password", "CHANGE_ME");

        return props;
    }

    /**
     * SASL/OAUTHBEARER: delegates auth to an external OAuth/OIDC identity
     * provider (Okta, Azure AD, Keycloak, etc.) — the client fetches a
     * short-lived bearer token and presents it instead of a static
     * username/password. This is the mechanism of choice for centralized
     * identity / SSO-integrated Kafka access in larger orgs.
     */
    public static Properties saslOAuthConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "broker1:9094");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "OAUTHBEARER");

        // The callback handler is what actually fetches/refreshes the token
        // from your identity provider's token endpoint.
        props.put("sasl.login.callback.handler.class",
                "org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler");

        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
                "clientId=\"CHANGE_ME\" " +
                "clientSecret=\"CHANGE_ME\" " +
                "tokenEndpointUrl=\"https://your-idp.example.com/oauth2/token\";");

        props.put("ssl.truststore.location", "/path/to/client.truststore.jks");
        props.put("ssl.truststore.password", "CHANGE_ME");

        return props;
    }

    public static void main(String[] args) {
        System.out.println("=== SASL/PLAIN (template - must pair with SSL) ===");
        saslPlainConfig().forEach((k, v) -> System.out.println(k + " = " + v));

        System.out.println("\n=== SASL/SCRAM-SHA-512 (template) ===");
        saslScramConfig().forEach((k, v) -> System.out.println(k + " = " + v));

        System.out.println("\n=== SASL/OAUTHBEARER (template) ===");
        saslOAuthConfig().forEach((k, v) -> System.out.println(k + " = " + v));

        System.out.println("\nMechanism choice guide:");
        System.out.println(" PLAIN  - simplest, fine for internal/dev if paired with SSL; static creds.");
        System.out.println(" SCRAM  - static creds but password never travels over the wire; broker-managed.");
        System.out.println(" OAUTHBEARER - SSO/centralized identity, short-lived tokens; best for larger orgs.");
    }
}
