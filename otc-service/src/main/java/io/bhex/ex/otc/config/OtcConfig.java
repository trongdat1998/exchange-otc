package io.bhex.ex.otc.config;

import java.util.List;

import com.google.common.collect.Lists;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "otc")
public class OtcConfig {

    private int stubDeadline = 0;

    private List<ChannelInfo> channelInfo = Lists.newArrayList();

}
