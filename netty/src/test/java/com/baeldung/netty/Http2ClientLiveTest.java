package com.baeldung.netty;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.InputStream;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledHeapByteBuf;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http2.HttpConversionUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baeldung.netty.http2.Http2Util;
import com.baeldung.netty.http2.client.Http2ClientInitializer;
import com.baeldung.netty.http2.client.Http2ClientResponseHandler;
import com.baeldung.netty.http2.client.Http2SettingsHandler;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;

//Ensure the server class - Http2Server.java is already started before running this test
public class Http2ClientLiveTest {

    private static final Logger logger = LoggerFactory.getLogger(Http2ClientLiveTest.class);

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 8443;
    private final URI configUri = URI.create("https://"+HOST+":"+PORT);
    private SslContext sslCtx;
    private Channel channel;
    public final int indexGranularity = 131072;
    public final String HeaderEMCExtensionIndexGranularity = "x-emc-index-granularity";
    @Before
    public void setup() throws Exception {
        sslCtx = Http2Util.createSSLContext(false);
    }

    @Test
    public void whenRequestSent_thenHelloWorldReceived() throws Exception {

        EventLoopGroup workerGroup = new NioEventLoopGroup();
        Http2ClientInitializer initializer = new Http2ClientInitializer(sslCtx, Integer.MAX_VALUE, HOST, PORT);

        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.remoteAddress(HOST, PORT);
            b.handler(initializer);

            channel = b.connect()
                .syncUninterruptibly()
                .channel();

            logger.info("Connected to [" + HOST + ':' + PORT + ']');

            Http2SettingsHandler http2SettingsHandler = initializer.getSettingsHandler();
            http2SettingsHandler.awaitSettings(60, TimeUnit.SECONDS);

            logger.info("Sending request(s)...");

//            FullHttpRequest request = Http2Util.createGetRequest(HOST, PORT);

            byte[] content = new byte[1024];
            putObject("dummy-bucket", "dummy-key", 1024, content, initializer.getResponseHandler());
//            responseHandler.put(streamId, channel.write(request), channel.newPromise());
//            channel.flush();
//            String response = responseHandler.awaitResponses(60, TimeUnit.SECONDS);

//            assertEquals("Hello World", response);

            logger.info("Finished HTTP/2 request(s)");

        } finally {
            workerGroup.shutdownGracefully();
        }

    }

    @After
    public void cleanup() {
        channel.close()
            .syncUninterruptibly();
    }

    public boolean putObject(String bucket, String key, int length, byte[] content, Http2ClientResponseHandler responseHandler) {
        try {
            String streamId = bucket + key;
            DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.valueOf("HTTP/2.0"), HttpMethod.PUT, "/" + bucket + "/" + key, Unpooled.wrappedBuffer(content));
            request.headers()
                    .set(HttpHeaderNames.HOST, configUri)
                    .set(HttpHeaderNames.ACCEPT, "*/*")
                    .set(HttpHeaderNames.CONTENT_LENGTH, length)
                    .set(HttpHeaderNames.CONTENT_TYPE, "application/octet-stream")
                    .set(HeaderEMCExtensionIndexGranularity, indexGranularity)
                    .set(HttpConversionUtil.ExtensionHeaderNames.SCHEME.text(), HttpScheme.HTTPS)
                    .set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.GZIP)
                    .set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.DEFLATE)
                    .set(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text(), streamId);
            responseHandler.put(streamId, channel.write(request), channel.newPromise());
            channel.flush();
            HttpResponse response = responseHandler.awaitResponse(streamId, 60, TimeUnit.SECONDS);
            return response.status() == HttpResponseStatus.OK;
        } catch (Exception e) {
            logger.error("put object {} {} content {} failed", bucket, key, length, e);
        }
        return false;
    }
}
