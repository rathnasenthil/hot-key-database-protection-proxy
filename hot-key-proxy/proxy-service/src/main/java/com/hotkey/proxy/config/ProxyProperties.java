package com.hotkey.proxy.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ConfigurationProperties(prefix = "proxy")
public class ProxyProperties {

    private String instanceId = "proxy";
    private String originBaseUrl = "http://localhost:8081";
    private List<String> redisNodes = new ArrayList<>(List.of("localhost:6379"));
    private Duration commandTimeout = Duration.ofMillis(200);
    private Duration negativeTtl = Duration.ofSeconds(5);
    private double ttlJitterFraction = 0.1;
    private int retryAfterSeconds = 1;
    private Map<String, Policy> policies = defaultPolicies();
    private Coalescing coalescing = new Coalescing();
    private HotKey hotKey = new HotKey();
    private Admission admission = new Admission();
    private Hashing hashing = new Hashing();

    private static Map<String, Policy> defaultPolicies() {
        Map<String, Policy> policies = new LinkedHashMap<>();
        policies.put("product", policy(Duration.ofSeconds(60), Duration.ofSeconds(30), Duration.ofSeconds(300)));
        policies.put("reviews", policy(Duration.ofSeconds(30), Duration.ofSeconds(15), Duration.ofSeconds(120)));
        return policies;
    }

    private static Policy policy(Duration ttl, Duration stale, Duration hot) {
        Policy policy = new Policy();
        policy.setTtl(ttl);
        policy.setStaleWindow(stale);
        policy.setHotTtl(hot);
        return policy;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getOriginBaseUrl() {
        return originBaseUrl;
    }

    public void setOriginBaseUrl(String originBaseUrl) {
        this.originBaseUrl = originBaseUrl;
    }

    public List<String> getRedisNodes() {
        return redisNodes;
    }

    public void setRedisNodes(List<String> redisNodes) {
        if (redisNodes == null) {
            this.redisNodes = List.of();
            return;
        }
        this.redisNodes = redisNodes.stream()
                .flatMap(value -> Arrays.stream(value.split(",")))
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .toList();
    }

    public Duration getCommandTimeout() {
        return commandTimeout;
    }

    public void setCommandTimeout(Duration commandTimeout) {
        this.commandTimeout = commandTimeout;
    }

    public Duration getNegativeTtl() {
        return negativeTtl;
    }

    public void setNegativeTtl(Duration negativeTtl) {
        this.negativeTtl = negativeTtl;
    }

    public double getTtlJitterFraction() {
        return ttlJitterFraction;
    }

    public void setTtlJitterFraction(double ttlJitterFraction) {
        this.ttlJitterFraction = ttlJitterFraction;
    }

    public int getRetryAfterSeconds() {
        return retryAfterSeconds;
    }

    public void setRetryAfterSeconds(int retryAfterSeconds) {
        this.retryAfterSeconds = retryAfterSeconds;
    }

    public Map<String, Policy> getPolicies() {
        return policies;
    }

    public void setPolicies(Map<String, Policy> policies) {
        this.policies = policies;
    }

    public Coalescing getCoalescing() {
        return coalescing;
    }

    public void setCoalescing(Coalescing coalescing) {
        this.coalescing = coalescing;
    }

    public HotKey getHotKey() {
        return hotKey;
    }

    public void setHotKey(HotKey hotKey) {
        this.hotKey = hotKey;
    }

    public Admission getAdmission() {
        return admission;
    }

    public void setAdmission(Admission admission) {
        this.admission = admission;
    }

    public Hashing getHashing() {
        return hashing;
    }

    public void setHashing(Hashing hashing) {
        this.hashing = hashing;
    }

    public static class Policy {
        private Duration ttl = Duration.ofSeconds(60);
        private Duration staleWindow = Duration.ofSeconds(30);
        private Duration hotTtl = Duration.ofSeconds(300);

        public Duration getTtl() {
            return ttl;
        }

        public void setTtl(Duration ttl) {
            this.ttl = ttl;
        }

        public Duration getStaleWindow() {
            return staleWindow;
        }

        public void setStaleWindow(Duration staleWindow) {
            this.staleWindow = staleWindow;
        }

        public Duration getHotTtl() {
            return hotTtl;
        }

        public void setHotTtl(Duration hotTtl) {
            this.hotTtl = hotTtl;
        }
    }

    public static class Coalescing {
        private Duration maxWait = Duration.ofMillis(50);

        public Duration getMaxWait() {
            return maxWait;
        }

        public void setMaxWait(Duration maxWait) {
            this.maxWait = maxWait;
        }
    }

    public static class HotKey {
        private double threshold = 500;
        private Duration window = Duration.ofSeconds(5);
        private double demoteFraction = 0.5;

        public double getThreshold() {
            return threshold;
        }

        public void setThreshold(double threshold) {
            this.threshold = threshold;
        }

        public Duration getWindow() {
            return window;
        }

        public void setWindow(Duration window) {
            this.window = window;
        }

        public double getDemoteFraction() {
            return demoteFraction;
        }

        public void setDemoteFraction(double demoteFraction) {
            this.demoteFraction = demoteFraction;
        }
    }

    public static class Admission {
        private int maxOriginConcurrency = 20;
        private int queueLimit = 100;
        private Duration queueWait = Duration.ofMillis(100);
        private int unhealthyMinSamples = 10;
        private double unhealthyFailureRate = 0.5;

        public int getMaxOriginConcurrency() {
            return maxOriginConcurrency;
        }

        public void setMaxOriginConcurrency(int maxOriginConcurrency) {
            this.maxOriginConcurrency = maxOriginConcurrency;
        }

        public int getQueueLimit() {
            return queueLimit;
        }

        public void setQueueLimit(int queueLimit) {
            this.queueLimit = queueLimit;
        }

        public Duration getQueueWait() {
            return queueWait;
        }

        public void setQueueWait(Duration queueWait) {
            this.queueWait = queueWait;
        }

        public int getUnhealthyMinSamples() {
            return unhealthyMinSamples;
        }

        public void setUnhealthyMinSamples(int unhealthyMinSamples) {
            this.unhealthyMinSamples = unhealthyMinSamples;
        }

        public double getUnhealthyFailureRate() {
            return unhealthyFailureRate;
        }

        public void setUnhealthyFailureRate(double unhealthyFailureRate) {
            this.unhealthyFailureRate = unhealthyFailureRate;
        }
    }

    public static class Hashing {
        private int virtualNodesPerNode = 150;

        public int getVirtualNodesPerNode() {
            return virtualNodesPerNode;
        }

        public void setVirtualNodesPerNode(int virtualNodesPerNode) {
            this.virtualNodesPerNode = virtualNodesPerNode;
        }
    }
}
