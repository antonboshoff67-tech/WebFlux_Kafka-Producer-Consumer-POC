package com.antontech.webflux_kafka.model;

/** Wraps a signed JWT string returned to callers that need to forward requests. */
public class JwtResponse {
    private String token;

    public JwtResponse() {}
    public JwtResponse(String token) { this.token = token; }

    public String getToken() { return token; }
    public void setToken(String token) { this.token = token; }
}

