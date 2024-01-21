package io.bhex.ex.otc.config;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 服务配置信息
 *
 * @author lizhen
 * @date 2018-09-14
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ChannelInfo {

    private String channelName;

    private String host;

    private int port;

    private boolean useSsl;

    private String grpcKeyPath;

    private String grpcCrtPath;
}