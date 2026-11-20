/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.nats;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.nats.client.NKey;
import io.nats.client.support.Encoding;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Credentials and certificates for tests against a real nats-server. */
final class NatsSecurityFixtures {
  static final String STORE_PASSWORD = "changeit";

  private static final SecureRandom RANDOM = new SecureRandom();

  private NatsSecurityFixtures() {}

  /** An operator, one account and one user: the decentralised auth .creds files belong to. */
  static final class Operator {
    final String serverConfig;
    final String creds;

    private Operator(String serverConfig, String creds) {
      this.serverConfig = serverConfig;
      this.creds = creds;
    }
  }

  static Operator operator() throws Exception {
    NKey operator = NKey.createOperator(RANDOM);
    NKey account = NKey.createAccount(RANDOM);
    NKey user = NKey.createUser(RANDOM);
    String unlimited = "\"subs\":-1,\"data\":-1,\"payload\":-1";
    String operatorJwt =
        jwt(operator, publicKey(operator), "ol-operator", "{\"type\":\"operator\",\"version\":2}");
    String accountJwt =
        jwt(
            operator,
            publicKey(account),
            "ol-account",
            "{\"type\":\"account\",\"version\":2,\"limits\":{"
                + unlimited
                + ",\"imports\":-1,\"exports\":-1,\"wildcards\":true,\"conn\":-1,\"leaf\":-1}}");
    String userJwt =
        jwt(
            account,
            publicKey(user),
            "ol-user",
            "{\"type\":\"user\",\"version\":2,\"pub\":{},\"sub\":{}," + unlimited + "}");
    String serverConfig =
        "operator: "
            + operatorJwt
            + "\nresolver: MEMORY\nresolver_preload: { "
            + publicKey(account)
            + ": "
            + accountJwt
            + " }\n";
    String creds =
        "-----BEGIN NATS USER JWT-----\n"
            + userJwt
            + "\n------END NATS USER JWT------\n\n"
            + "-----BEGIN USER NKEY SEED-----\n"
            + new String(user.getSeed())
            + "\n------END USER NKEY SEED------\n";
    return new Operator(serverConfig, creds);
  }

  /** A NATS v2 JWT, encoded and signed the way nsc does. */
  private static String jwt(NKey signer, String subject, String name, String natsClaims)
      throws Exception {
    String claimsWithoutId =
        "{\"iat\":"
            + System.currentTimeMillis() / 1000
            + ",\"iss\":\""
            + publicKey(signer)
            + "\",\"name\":\""
            + name
            + "\",\"sub\":\""
            + subject
            + "\",\"nats\":"
            + natsClaims;
    byte[] digest =
        MessageDigest.getInstance("SHA-256")
            .digest((claimsWithoutId + "}").getBytes(StandardCharsets.UTF_8));
    String jti = new String(Encoding.base32Encode(digest)).replace("=", "");
    String claims = claimsWithoutId + ",\"jti\":\"" + jti + "\"}";
    String signingInput =
        Encoding.toBase64Url("{\"typ\":\"JWT\",\"alg\":\"ed25519-nkey\"}")
            + "."
            + Encoding.toBase64Url(claims);
    return signingInput
        + "."
        + Encoding.toBase64Url(signer.sign(signingInput.getBytes(StandardCharsets.UTF_8)));
  }

  private static String publicKey(NKey key) throws Exception {
    return new String(key.getPublicKey());
  }

  /** PEM files for nats-server plus PKCS12 stores for the Java client. */
  static final class Certificates {
    final Path serverCert;
    final Path serverKey;
    final Path wrongHostCert;
    final Path wrongHostKey;
    final Path dnsOnlyCert;
    final Path dnsOnlyKey;
    final Path ca;
    final Path truststore;
    final Path clientKeystore;

    private Certificates(Path dir) {
      this.serverCert = dir.resolve("server.pem");
      this.serverKey = dir.resolve("server.key");
      this.wrongHostCert = dir.resolve("wronghost.pem");
      this.wrongHostKey = dir.resolve("wronghost.key");
      this.dnsOnlyCert = dir.resolve("dnsonly.pem");
      this.dnsOnlyKey = dir.resolve("dnsonly.key");
      this.ca = dir.resolve("ca.pem");
      this.truststore = dir.resolve("truststore.p12");
      this.clientKeystore = dir.resolve("client.p12");
    }
  }

  static Certificates certificates(Path dir) throws Exception {
    assumeTrue(NatsTestServer.findOnPath("openssl").isPresent(), "needs openssl on PATH");
    run(
        dir,
        "openssl",
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-days",
        "1",
        "-subj",
        "/CN=ol-test-ca",
        "-addext",
        "basicConstraints=critical,CA:TRUE",
        "-addext",
        "keyUsage=critical,keyCertSign,cRLSign",
        "-keyout",
        "ca.key",
        "-out",
        "ca.pem");
    String leaf = "authorityKeyIdentifier=keyid,issuer\nkeyUsage=critical,digitalSignature\n";
    write(
        dir.resolve("server.ext"),
        leaf + "extendedKeyUsage=serverAuth\nsubjectAltName=IP:127.0.0.1,DNS:localhost\n");
    write(
        dir.resolve("dnsonly.ext"),
        leaf + "extendedKeyUsage=serverAuth\nsubjectAltName=DNS:localhost\n");
    write(
        dir.resolve("wronghost.ext"),
        leaf + "extendedKeyUsage=serverAuth\nsubjectAltName=DNS:not-this-host.example\n");
    write(dir.resolve("client.ext"), leaf + "extendedKeyUsage=clientAuth\n");
    for (String name : Arrays.asList("server", "wronghost", "dnsonly", "client")) {
      run(
          dir,
          "openssl",
          "req",
          "-newkey",
          "rsa:2048",
          "-nodes",
          "-subj",
          "/CN=ol-test-" + name,
          "-keyout",
          name + ".key",
          "-out",
          name + ".csr");
      run(
          dir,
          "openssl",
          "x509",
          "-req",
          "-in",
          name + ".csr",
          "-CA",
          "ca.pem",
          "-CAkey",
          "ca.key",
          "-CAcreateserial",
          "-days",
          "1",
          "-out",
          name + ".pem",
          "-extfile",
          name + ".ext");
    }
    run(
        dir,
        "openssl",
        "pkcs12",
        "-export",
        "-in",
        "client.pem",
        "-inkey",
        "client.key",
        "-out",
        "client.p12",
        "-passout",
        "pass:" + STORE_PASSWORD);
    run(
        dir,
        Paths.get(System.getProperty("java.home"), "bin", "keytool").toString(),
        "-importcert",
        "-noprompt",
        "-alias",
        "ol-test-ca",
        "-file",
        "ca.pem",
        "-keystore",
        "truststore.p12",
        "-storetype",
        "PKCS12",
        "-storepass",
        STORE_PASSWORD);
    return new Certificates(dir);
  }

  private static void write(Path file, String content) throws IOException {
    Files.write(file, content.getBytes(StandardCharsets.UTF_8));
  }

  private static void run(Path dir, String... command) throws Exception {
    List<String> args = new ArrayList<>(Arrays.asList(command));
    Process process =
        new ProcessBuilder(args)
            .directory(dir.toFile())
            .redirectErrorStream(true)
            .redirectOutput(dir.resolve("commands.log").toFile())
            .start();
    if (process.waitFor() != 0) {
      throw new IllegalStateException(
          "command failed: "
              + String.join(" ", args)
              + "\n"
              + new String(
                  Files.readAllBytes(dir.resolve("commands.log")), StandardCharsets.UTF_8));
    }
  }
}
