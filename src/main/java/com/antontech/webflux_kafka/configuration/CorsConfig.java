package com.antontech.webflux_kafka.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.reactive.CorsWebFilter;
import org.springframework.web.cors.reactive.UrlBasedCorsConfigurationSource;

import java.util.Arrays;
import java.util.List;

/**
 * WebFlux CORS configuration.
 *
 * <h2>WebFlux vs Spring MVC CORS</h2>
 * <p>
 * In Spring MVC you register a {@code WebMvcConfigurer#addCorsMappings} callback.
 * In Spring WebFlux there is no {@code WebMvcConfigurer} – instead you register a
 * {@link CorsWebFilter} bean, which is a Netty pipeline filter that intercepts
 * pre-flight (OPTIONS) and CORS requests before they reach your controllers.
 * </p>
 *
 * <h2>Why CORS matters</h2>
 * <p>
 * Browsers enforce the Same-Origin Policy: a React app running at
 * {@code http://localhost:5173} is a different origin from the backend at
 * {@code http://localhost:8083}. CORS headers tell the browser it is safe to
 * allow cross-origin requests. The backend adds these headers; the browser
 * enforces them.
 * </p>
 */
@Configuration
public class CorsConfig {

    /**
     * Comma-separated list of allowed origins (e.g.
     * {@code http://localhost:5173,http://localhost:3000,https://ui.example.com}).
     * Supplied via {@code cors.allowed-origins} in {@code application.yml} or the
     * {@code ITEM_CORS_ALLOWED_ORIGINS} environment variable.
     */
    @Value("${cors.allowed-origins}")
    private String allowedOrigins;

    /**
     * Registers a {@link CorsWebFilter} that permits the React frontends to call
     * this API from a browser. Applies to every path (/**).
     *
     * @return a configured {@link CorsWebFilter} bean wired into the Netty filter chain.
     */
    @Bean
    public CorsWebFilter corsWebFilter() {
        CorsConfiguration config = new CorsConfiguration();

        // Parse comma-separated origins
        List<String> origins = Arrays.asList(allowedOrigins.split(","));
        origins.replaceAll(String::trim);
        config.setAllowedOrigins(origins);

        config.setAllowedMethods(List.of("GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"));
        config.setAllowedHeaders(List.of("*"));
        config.setAllowCredentials(true);
        config.setMaxAge(3600L);

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", config);
        return new CorsWebFilter(source);
    }
}

