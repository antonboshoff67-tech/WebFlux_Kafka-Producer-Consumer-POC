package com.antontech.webflux_kafka.prop;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * SSL/TLS keystore properties for mutual-TLS gateway calls.
 * Bound from {@code keys.ssl.*} in {@code application.yml}.
 */
@Component
@ConfigurationProperties(prefix = "keys.ssl")
public class SSLProperties {
    private boolean enabled;
    private String keyStore;
    private String keyStorePassword;
    private String trustStore;
    private String trustStorePassword;

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public String getKeyStore() { return keyStore; }
    public void setKeyStore(String keyStore) { this.keyStore = keyStore; }
    public String getKeyStorePassword() { return keyStorePassword; }
    public void setKeyStorePassword(String keyStorePassword) { this.keyStorePassword = keyStorePassword; }
    public String getTrustStore() { return trustStore; }
    public void setTrustStore(String trustStore) { this.trustStore = trustStore; }
    public String getTrustStorePassword() { return trustStorePassword; }
    public void setTrustStorePassword(String trustStorePassword) { this.trustStorePassword = trustStorePassword; }
}

