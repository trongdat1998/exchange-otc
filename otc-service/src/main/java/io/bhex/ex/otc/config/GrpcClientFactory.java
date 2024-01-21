package io.bhex.ex.otc.config;

import org.apache.ibatis.javassist.CannotCompileException;
import org.apache.ibatis.javassist.ClassClassPath;
import org.apache.ibatis.javassist.ClassPool;
import org.apache.ibatis.javassist.CtClass;
import org.apache.ibatis.javassist.CtMethod;
import org.apache.ibatis.javassist.NotFoundException;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import io.bhex.base.account.BalanceChangeRecordServiceGrpc;
import io.bhex.base.account.BalanceServiceGrpc;
import io.bhex.base.grpc.client.channel.IGrpcClientPool;
import io.bhex.base.quote.QuoteServiceGrpc;
import io.bhex.base.rc.ExchangeRCServiceGrpc;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class GrpcClientFactory {

    private static final String BH_SERVER_CHANNEL_NAME = "bhServer";
    private static final String QUOTE_DATA_SERVER_CHANNEL_NAME = "quoteDataServer";

    private static IGrpcClientPool clientPool;

    @Resource
    private OtcConfig otcConfig;

    @Resource
    private IGrpcClientPool pool;

    @PostConstruct
    public void init() throws Exception {
        //grpcCallInitialized();

        clientPool = pool;
        List<ChannelInfo> channelInfoList = otcConfig.getChannelInfo();
        for (ChannelInfo channelInfo : channelInfoList) {
            pool.setShortcut(channelInfo.getChannelName(), channelInfo.getHost(), channelInfo.getPort());
        }
    }

    public BalanceChangeRecordServiceGrpc.BalanceChangeRecordServiceBlockingStub balanceChangeBlockingStub() {
        Channel channel = pool.borrowChannel(BH_SERVER_CHANNEL_NAME);
        return BalanceChangeRecordServiceGrpc.newBlockingStub(channel).withDeadlineAfter(otcConfig.getStubDeadline(),
                TimeUnit.MILLISECONDS);
    }

    public QuoteServiceGrpc.QuoteServiceBlockingStub quoteServiceBlockingStub() {
        Channel channel = pool.borrowChannel(QUOTE_DATA_SERVER_CHANNEL_NAME);
        return QuoteServiceGrpc.newBlockingStub(channel).withDeadlineAfter(otcConfig.getStubDeadline(),
                TimeUnit.MILLISECONDS);
    }

    public BalanceServiceGrpc.BalanceServiceBlockingStub balanceServiceBlockingStub() {
        Channel channel = pool.borrowChannel(BH_SERVER_CHANNEL_NAME);
        return BalanceServiceGrpc.newBlockingStub(channel).withDeadlineAfter(otcConfig.getStubDeadline(),
                TimeUnit.MILLISECONDS);
    }

    public ExchangeRCServiceGrpc.ExchangeRCServiceBlockingStub exchangeRCServiceBlockingStub() {
        Channel channel = pool.borrowChannel(BH_SERVER_CHANNEL_NAME);
        return ExchangeRCServiceGrpc.newBlockingStub(channel).withDeadlineAfter(otcConfig.getStubDeadline(), TimeUnit.MILLISECONDS);
    }

    /**
     * 添加ClientCalls执行grpc调用的钩子，执行完毕归还channel到pool
     */
    private void grpcCallInitialized() throws NotFoundException, CannotCompileException {
        ClassPool pool = ClassPool.getDefault();
        // 使用当前的类加载器
        pool.insertClassPath(new ClassClassPath(this.getClass()));
        CtClass clientCalls = pool.get("io.grpc.stub.ClientCalls");
        // 获得第二个重载的grpc调用接口
        CtMethod blockingUnaryCall = clientCalls.getDeclaredMethods("blockingUnaryCall")[1];
        // 钩子入口，在此加入需要执行的代码
        blockingUnaryCall.insertAfter("{ io.bhex.ex.otc.config.GrpcClientFactory.returnChannel(channel); }");
        clientCalls.toClass();
    }

    public static void returnChannel(Channel channel) {
        // 将channel放回pool
        if (channel != null) {
            clientPool.returnChannel((ManagedChannel) channel);
        }
    }
}


