package com.moap.dataprocess.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@ConfigurationProperties(prefix = "sysconfig")
@Component
@Data
public class SystemConfig {
    public String indexFilePath;
    public String dataFilePath;
    public String moapListUrl;
    public String moapDataUrl;
}
