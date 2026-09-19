package io.sparkconnect.propagation;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;

/**
 * Plugin configuration, read from the Spark conf.
 *
 * <p>This has to read its own configuration rather than receive it, because
 * {@code SparkConnectInterceptorRegistry.createInstance} constructs interceptors reflectively
 * via a <em>zero-argument</em> constructor and throws {@code CONNECT.INTERCEPTOR_CTOR_MISSING}
 * otherwise.
 *
 * <p>Secrets come from the environment rather than the Spark conf, so they do not appear in
 * the Spark UI's environment tab.
 */
public final class PluginConfig {

  private static final String PREFIX = "spark.connect.propagation.";

  public final String issuer;
  public final String jwksUri;
  public final String tokenEndpoint;
  public final String expectedAudience;
  public final String exchangeClientId;
  public final String exchangeClientSecret;
  public final String exchangeAudience;
  public final String tokenHeader;
  public final String correlationHeader;
  public final long clockSkewSeconds;

  private PluginConfig(SparkConf conf) {
    this.issuer = require(conf, "oidc.issuer");
    this.jwksUri = require(conf, "oidc.jwks-uri");
    this.tokenEndpoint = require(conf, "oidc.token-endpoint");
    this.expectedAudience = conf.get(PREFIX + "oidc.expected-audience", "spark-connect");
    this.exchangeClientId = conf.get(PREFIX + "exchange.client-id", "spark-connect");
    this.exchangeAudience = conf.get(PREFIX + "exchange.audience", "polaris");
    this.tokenHeader = conf.get(PREFIX + "header.token", "x-user-token");
    this.correlationHeader = conf.get(PREFIX + "header.correlation-id", "x-correlation-id");
    this.clockSkewSeconds = Long.parseLong(conf.get(PREFIX + "exchange.skew-seconds", "30"));

    String secret = System.getenv("SPARK_CONNECT_PROPAGATION_CLIENT_SECRET");
    if (secret == null || secret.isBlank()) {
      throw new IllegalStateException(
          "SPARK_CONNECT_PROPAGATION_CLIENT_SECRET is not set; the Connect server cannot "
              + "authenticate to the identity provider to perform the token exchange.");
    }
    this.exchangeClientSecret = secret;
  }

  private static String require(SparkConf conf, String suffix) {
    String value = conf.get(PREFIX + suffix, null);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException("Missing required configuration: " + PREFIX + suffix);
    }
    return value;
  }

  public static PluginConfig fromSparkEnv() {
    SparkEnv env = SparkEnv.get();
    if (env == null) {
      throw new IllegalStateException("SparkEnv is not initialised; cannot read plugin config");
    }
    return new PluginConfig(env.conf());
  }
}
