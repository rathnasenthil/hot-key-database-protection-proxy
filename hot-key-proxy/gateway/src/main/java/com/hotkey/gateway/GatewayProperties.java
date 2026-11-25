package com.hotkey.gateway;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@ConfigurationProperties(prefix = "gateway")
public class GatewayProperties {

    private List<String> proxies = new ArrayList<>();
    private String originBaseUrl = "http://localhost:8081";
    private String selfBaseUrl = "http://127.0.0.1:8080";
    private String prometheusUrl = "http://localhost:9090";
    private int virtualNodes = 150;
    private int productCount = 100_000;

    public List<String> getProxies() {
        return proxies;
    }

    public void setProxies(List<String> proxies) {
        if (proxies == null) {
            this.proxies = List.of();
            return;
        }
        this.proxies = proxies.stream()
                .flatMap(value -> Arrays.stream(value.split(",")))
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .map(GatewayProperties::stripTrailingSlash)
                .toList();
    }

    public String getOriginBaseUrl() {
        return originBaseUrl;
    }

    public void setOriginBaseUrl(String originBaseUrl) {
        this.originBaseUrl = stripTrailingSlash(originBaseUrl);
    }

    public String getSelfBaseUrl() {
        return selfBaseUrl;
    }

    public void setSelfBaseUrl(String selfBaseUrl) {
        this.selfBaseUrl = stripTrailingSlash(selfBaseUrl);
    }

    public String getPrometheusUrl() {
        return prometheusUrl;
    }

    public void setPrometheusUrl(String prometheusUrl) {
        this.prometheusUrl = stripTrailingSlash(prometheusUrl);
    }

    public int getVirtualNodes() {
        return virtualNodes;
    }

    public void setVirtualNodes(int virtualNodes) {
        this.virtualNodes = virtualNodes;
    }

    public int getProductCount() {
        return productCount;
    }

    public void setProductCount(int productCount) {
        this.productCount = productCount;
    }

    private static String stripTrailingSlash(String value) {
        if (value != null && value.endsWith("/")) {
            return value.substring(0, value.length() - 1);
        }
        return value;
    }
}
