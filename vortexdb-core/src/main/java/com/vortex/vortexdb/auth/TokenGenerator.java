
package com.vortex.vortexdb.auth;

import com.vortex.vortexdb.config.AuthOptions;
import com.vortex.common.config.VortexConfig;
import io.jsonwebtoken.*;
import io.jsonwebtoken.security.Keys;

import javax.crypto.SecretKey;
import javax.ws.rs.NotAuthorizedException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;

public class TokenGenerator {

    private final SecretKey key;

    public TokenGenerator(VortexConfig config) {
        String secretKey = config.get(AuthOptions.AUTH_TOKEN_SECRET);
        this.key = Keys.hmacShaKeyFor(secretKey.getBytes(StandardCharsets.UTF_8));
    }

    public String create(Map<String, ?> payload, long expire) {
        return Jwts.builder()
                   .setClaims(payload)
                   .setExpiration(new Date(System.currentTimeMillis() + expire))
                   .signWith(this.key, SignatureAlgorithm.HS256)
                   .compact();
    }

    public Claims verify(String token) {
        try {
            Jws<Claims> claimsJws = Jwts.parserBuilder()
                                        .setSigningKey(key)
                                        .build()
                                        .parseClaimsJws(token);
            return claimsJws.getBody();
        } catch (ExpiredJwtException e) {
            throw new NotAuthorizedException("The token is expired", e);
        } catch (JwtException e) {
            throw new NotAuthorizedException("Invalid token", e);
        }
    }
}
