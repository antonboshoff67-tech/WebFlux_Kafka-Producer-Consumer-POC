package com.antontech.webflux_kafka.util;

import io.jsonwebtoken.Header;
import io.jsonwebtoken.JwsHeader;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.security.Key;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.Security;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Builds and signs short-lived JWTs used to authenticate outbound calls from
 * this service to the downstream gateway endpoint, and parses PEM-encoded
 * RSA private keys supplied via configuration.
 * <p>
 * See {@code SETUP_GUIDE.md} for step-by-step instructions on generating a
 * key pair and configuring {@code jwt.private-key} / {@code ITEM_JWT_PRIVATE_KEY}.
 */
@Slf4j
@Component
public class JwtTokenUtil implements Serializable {

  @Value("${jwt.private-key:}")
  private String privateKey;

  @Value("${jwt.issuer:item-kafka-producer}")
  private String issuer;

  @Value("${jwt.expiry-minutes:30}")
  private int expiryMinutes;

  /**
   * Builds a short-lived, RS256-signed JWT for authenticating outbound calls
   * to the gateway endpoint. The signing key is read from
   * {@code jwt.private-key} (see {@code SETUP_GUIDE.md} for how to generate
   * and configure this).
   *
   * @return the compact JWT string.
   * @throws IllegalStateException if no private key is configured, or the token cannot be built.
   */
  public String buildGatewayToken() {
    if (privateKey == null || privateKey.isBlank()) {
      throw new IllegalStateException("JWT private key is not configured. Set ITEM_JWT_PRIVATE_KEY or jwt.private-key.");
    }

    Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.MINUTE, expiryMinutes);

    Map<String, Object> header = new HashMap<>();
    header.put(Header.TYPE, Header.JWT_TYPE);
    header.put(JwsHeader.ALGORITHM, SignatureAlgorithm.RS256);
    header.put(JwsHeader.KEY_ID, issuer + ".1");

    try {
      return Jwts.builder()
          .setHeader(header)
          .setId(UUID.randomUUID().toString())
          .setAudience("/item-kafka/app/send-items/v1")
          .setIssuer(issuer)
          .setExpiration(calendar.getTime())
          .signWith(SignatureAlgorithm.RS256, getPrivateKey(privateKey))
          .compact();
    } catch (IOException e) {
      log.error("Unable to build JWT token", e);
      throw new IllegalStateException("Unable to build JWT token", e);
    }
  }

  /**
   * Parses a PEM-encoded RSA private key (either PKCS#1 "RSA PRIVATE KEY" or
   * PKCS#8 "PRIVATE KEY" format) into a {@link Key} usable for JWT signing.
   * Accepts either the full PEM block (with {@code BEGIN}/{@code END}
   * markers) or just the raw base64 body, in which case a PKCS#1 wrapper is
   * assumed.
   *
   * @param privateKeyString the PEM content or raw base64 key body.
   * @return the parsed private {@link Key}.
   * @throws IOException              if the PEM content cannot be parsed.
   * @throws IllegalArgumentException if the content is empty or not a supported PEM key type.
   */
  public Key getPrivateKey(String privateKeyString) throws IOException {
    if (privateKeyString == null || privateKeyString.isBlank()) {
      throw new IllegalArgumentException("Private key content is empty.");
    }

    if (Security.getProvider("BC") == null) {
      Security.addProvider(new BouncyCastleProvider());
    }

    String pem = privateKeyString.contains("BEGIN")
        ? privateKeyString
        : String.join(System.lineSeparator(),
            "-----BEGIN RSA PRIVATE KEY-----",
            privateKeyString,
            "-----END RSA PRIVATE KEY-----");

    try (PEMParser pemParser = new PEMParser(new StringReader(pem))) {
      Object object = pemParser.readObject();
      JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider("BC");

      if (object instanceof PEMKeyPair) {
        KeyPair keyPair = converter.getKeyPair((PEMKeyPair) object);
        return keyPair.getPrivate();
      }

      if (object instanceof PrivateKeyInfo) {
        PrivateKey privateKey = converter.getPrivateKey((PrivateKeyInfo) object);
        return privateKey;
      }

      throw new IllegalArgumentException("Configured JWT private key is not a supported PEM format.");
    }
  }
}


