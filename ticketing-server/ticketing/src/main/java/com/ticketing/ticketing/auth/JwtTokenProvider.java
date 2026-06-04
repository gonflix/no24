// package com.ticketing.ticketing.auth;

// import java.nio.charset.StandardCharsets;

// import javax.crypto.SecretKey;

// import org.springframework.stereotype.Component;

// import com.ticketing.ticketing.config.AppProperties;

// import io.jsonwebtoken.Claims;
// import io.jsonwebtoken.JwtException;
// import io.jsonwebtoken.Jwts;
// import io.jsonwebtoken.security.Keys;
// import jakarta.annotation.PostConstruct;
// import lombok.extern.slf4j.Slf4j;

// @Slf4j
// @Component
// public class JwtTokenProvider {

// private final AppProperties appProperties;
// private SecretKey secretKey;

// public JwtTokenProvider(AppProperties appProperties) {
// this.appProperties = appProperties;
// }

// @PostConstruct
// public void init() {
// this.secretKey =
// Keys.hmacShaKeyFor(appProperties.jwt().secret().getBytes(StandardCharsets.UTF_8));
// }

// public String extractUserId(String token) {
// try {
// Claims claims =
// Jwts.parser().verifyWith(secretKey).build().parseSignedClaims(token).getPayload();
// return claims.getSubject();
// } catch (JwtException | IllegalArgumentException ex) {
// log.error("Failed to extract userId from token:{}, {}", token,
// ex.getMessage());
// return null;
// }
// }
// }
