package com.zdjizhi.utils;

import com.zdjizhi.common.CommonConfig;
import org.apache.http.*;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * http client工具类
 * @author wlh
 */
public class HttpClientUtils {
    /** 全局连接池对象 */
    private static final PoolingHttpClientConnectionManager CONN_MANAGER = new PoolingHttpClientConnectionManager();

    private static Logger logger = LoggerFactory.getLogger(HttpClientUtils.class);
    public static final String ERROR_MESSAGE = "-1";

    /*
     * 静态代码块配置连接池信息
     */
    static {

        // 设置最大连接数
        CONN_MANAGER.setMaxTotal(CommonConfig.HTTP_POOL_MAX_CONNECTION);
        // 设置每个连接的路由数
        CONN_MANAGER.setDefaultMaxPerRoute(CommonConfig.HTTP_POOL_MAX_PER_ROUTE);

    }

    /**
     * 获取Http客户端连接对象
     * @return Http客户端连接对象
     */
    private static CloseableHttpClient getHttpClient() {
        // 创建Http请求配置参数
        RequestConfig requestConfig = RequestConfig.custom()
                // 获取连接超时时间
                .setConnectionRequestTimeout(CommonConfig.HTTP_POOL_REQUEST_TIMEOUT)
                // 请求超时时间
                .setConnectTimeout(CommonConfig.HTTP_POOL_CONNECT_TIMEOUT)
                // 响应超时时间
                .setSocketTimeout(CommonConfig.HTTP_POOL_RESPONSE_TIMEOUT)
                .build();

        /*
         * 测出超时重试机制为了防止超时不生效而设置
         *  如果直接放回false,不重试
         *  这里会根据情况进行判断是否重试
         */
        HttpRequestRetryHandler retry = (exception, executionCount, context) -> {
            if (executionCount >= 3) {// 如果已经重试了3次，就放弃
                return false;
            }
            if (exception instanceof NoHttpResponseException) {// 如果服务器丢掉了连接，那么就重试
                return true;
            }
            if (exception instanceof SSLHandshakeException) {// 不要重试SSL握手异常
                return false;
            }
            if (exception instanceof UnknownHostException) {// 目标服务器不可达
                return false;
            }
            if (exception instanceof ConnectTimeoutException) {// 连接被拒绝
                return false;
            }
            if (exception instanceof HttpHostConnectException) {// 连接被拒绝
                return false;
            }
            if (exception instanceof SSLException) {// ssl握手异常
                return false;
            }
            if (exception instanceof InterruptedIOException) {// 超时
                return true;
            }
            HttpClientContext clientContext = HttpClientContext.adapt(context);
            HttpRequest request = clientContext.getRequest();
            // 如果请求是幂等的，就再次尝试
            return !(request instanceof HttpEntityEnclosingRequest);
        };


        ConnectionKeepAliveStrategy myStrategy = (response, context) -> {
            HeaderElementIterator it = new BasicHeaderElementIterator
                    (response.headerIterator(HTTP.CONN_KEEP_ALIVE));
            while (it.hasNext()) {
                HeaderElement he = it.nextElement();
                String param = he.getName();
                String value = he.getValue();
                if (value != null && "timeout".equalsIgnoreCase(param)) {
                    return Long.parseLong(value) * 1000;
                }
            }
            return 60 * 1000;//如果没有约定，则默认定义时长为60s
        };

        // 创建httpClient
        return HttpClients.custom()
                // 把请求相关的超时信息设置到连接客户端
                .setDefaultRequestConfig(requestConfig)
                // 把请求重试设置到连接客户端
                .setRetryHandler(retry)
                .setKeepAliveStrategy(myStrategy)
                // 配置连接池管理对象
                .setConnectionManager(CONN_MANAGER)
                .build();
    }


    /**
     * GET请求
     *
     * @param uri 请求地
     * @return message
     */
    public static String httpGet(URI uri, Header... headers) {
        String msg = ERROR_MESSAGE;

        // 获取客户端连接对象
        CloseableHttpClient httpClient = getHttpClient();
        CloseableHttpResponse response = null;

        try {
            logger.info("http get uri {}",uri);
            // 创建GET请求对象
            HttpGet httpGet = new HttpGet(uri);
            
            if (StringUtil.isNotEmpty(headers)) {
                for (Header h : headers) {
                	httpGet.addHeader(h);
                    logger.info("request header : {}",h);
                }
            }
            // 执行请求
            response = httpClient.execute(httpGet);
            int statusCode = response.getStatusLine().getStatusCode();
            // 获取响应实体
            HttpEntity entity = response.getEntity();
            // 获取响应信息
            msg = EntityUtils.toString(entity, "UTF-8");

            if (statusCode != HttpStatus.SC_OK) {
                logger.error("Http get content is :{}" , msg);
            }

        } catch (ClientProtocolException e) {
            logger.error("协议错误: {}", e.getMessage());
        } catch (ParseException e) {
            logger.error("解析错误: {}", e.getMessage());
        } catch (IOException e) {
            logger.error("IO错误: {}",e.getMessage());
        }  finally {
            if (null != response) {
                try {
                    EntityUtils.consume(response.getEntity());
                    response.close();
                } catch (IOException e) {
                    logger.error("释放链接错误: {}", e.getMessage());

                }
            }
        }

        return msg;
    }
    /**
     * POST 请求
     * @param uri uri参数
     * @param requestBody 请求体
     * @return post请求返回结果
     */
    public static String httpPost(URI uri, String requestBody, Header... headers) {
    	String msg = ERROR_MESSAGE;
    	// 获取客户端连接对象
        CloseableHttpClient httpClient = getHttpClient();
        
        // 创建POST请求对象
        CloseableHttpResponse response = null;
        try {

            logger.info("http post uri:{}， http post body:{}", uri, requestBody);
            
            HttpPost httpPost = new HttpPost(uri);
            httpPost.setHeader("Content-Type", "application/x-www-form-urlencoded");
            if (StringUtil.isNotEmpty(headers)) {
                for (Header h : headers) {
                    httpPost.addHeader(h);
                    logger.info("request header : {}",h);
                }
            }

            if(StringUtil.isNotBlank(requestBody)) {
                byte[] bytes = requestBody.getBytes(StandardCharsets.UTF_8);
                httpPost.setEntity(new ByteArrayEntity(bytes));
            }
            
            response = httpClient.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            // 获取响应实体
            HttpEntity entity = response.getEntity();
            // 获取响应信息
            msg = EntityUtils.toString(entity, "UTF-8");
            
            if (statusCode != HttpStatus.SC_OK) {
                logger.error("Http post content is :{}" , msg);
            }
        } catch (ClientProtocolException e) {
            logger.error("协议错误: {}", e.getMessage());
        } catch (ParseException e) {
        	logger.error("解析错误: {}", e.getMessage());
        } catch (IOException e) {
        	logger.error("IO错误: {}", e.getMessage());
        } finally {
            if (null != response) {
                try {
                    EntityUtils.consumeQuietly(response.getEntity());
                    response.close();
                } catch (IOException e) {
                	logger.error("释放链接错误: {}", e.getMessage());

                }
            }
        }
        return msg;
    }

    /**
     * 拼装url
     * url ,参数map
     */
    public static void setUrlWithParams(URIBuilder uriBuilder,String path, Map<String, Object> params) {
        try {
            uriBuilder.setPath(path);
            if (params != null && !params.isEmpty()){
                for (Map.Entry<String, Object> kv : params.entrySet()) {
                    uriBuilder.setParameter(kv.getKey(),kv.getValue().toString());
                }
            }
        } catch (Exception e) {
            logger.error("拼接url出错,uri : {}, path : {},参数: {}",uriBuilder.toString(),path,params);
        }
    }

}
