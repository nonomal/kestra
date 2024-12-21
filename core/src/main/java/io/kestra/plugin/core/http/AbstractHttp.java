package io.kestra.plugin.core.http;

import com.google.common.base.Charsets;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.MediaType;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.classic.methods.HttpUriRequest;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.entity.UrlEncodedFormEntity;
import org.apache.hc.client5.http.entity.mime.FileBody;
import org.apache.hc.client5.http.entity.mime.MultipartEntityBuilder;
import org.apache.hc.client5.http.entity.mime.StringBody;
import org.apache.hc.client5.http.impl.DefaultAuthenticationStrategy;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.core5.http.*;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.*;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.ssl.SSLContexts;
import org.apache.hc.core5.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import static io.kestra.core.utils.Rethrow.throwFunction;

@Slf4j
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract public class AbstractHttp extends Task implements HttpInterface {
    @NotNull
    protected String uri;

    @Builder.Default
    protected HttpMethod method = HttpMethod.GET;

    protected String body;

    protected Map<String, Object> formData;

    @Builder.Default
    protected String contentType = MediaType.APPLICATION_JSON;

    protected Map<CharSequence, CharSequence> headers;

    protected RequestOptions options;

    protected SslOptions sslOptions;

    @Builder.Default
    private Boolean allowFailed = false;

    private SSLConnectionSocketFactory selfSignedConnectionSocketFactory() {
        try {
            SSLContext sslContext = SSLContexts
                .custom()
                .loadTrustMaterial(null, (chain, authType) -> true)
                .build();

            return new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE);
        } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static class CustomSocketFactory extends SSLConnectionSocketFactory {
        private RunContext runContext;
        private RequestOptions options;

        public CustomSocketFactory(final SSLContext sslContext, final HostnameVerifier hostnameVerifier) {
            super(sslContext, hostnameVerifier);
        }

        @Override
        public Socket createSocket(final HttpContext context) throws IOException {
            try {
                SocketAddress proxyAddr = new InetSocketAddress(
                    runContext.render(this.options.getProxyAddress()),
                    this.options.getProxyPort()
                );

                Proxy proxy = new Proxy(this.options.getProxyType(), proxyAddr);

                return new Socket(proxy);
            } catch (IllegalVariableEvaluationException e) {
                throw new IOException(e);
            }
        }

    }

    @AllArgsConstructor
    private static class LoggingRequestInterceptor implements HttpRequestInterceptor {
        Logger logger;
        Level logLevel;

        @Override
        public void process(HttpRequest request, EntityDetails entity, HttpContext context) throws HttpException, IOException {
            String message = "Request => " +
                buildRequestEntry(request) +
                (logLevel.compareTo(Level.DEBUG) >= 0 ? buildHeadersEntry(request.getHeaders()) : "") +
                (logLevel.compareTo(Level.TRACE) >= 0 ? buildEntityEntry(request) : "");

            logger.info(message);
        }

        private String buildRequestEntry(HttpRequest request) {
            return "\n  URI => "
                + request.getMethod() + " "
                + request.getRequestUri();
        }

        private String buildHeadersEntry(Header[] headers) {
            return "\n  Headers => "
                + Arrays.stream(headers)
                .map(header -> header.getName() + ": " + header.getValue())
                .collect(Collectors.joining(", "));
        }

        private String buildEntityEntry(HttpRequest request) throws IOException {
            if (request instanceof BasicClassicHttpRequest basic) {
                HttpEntity entity = basic.getEntity();
                ByteArrayOutputStream bs = new ByteArrayOutputStream();
                entity.writeTo(bs);
                return "\n  Payload => " + bs;
            } else {
                return "\n  Payload => (empty body)";
            }
        }
    }

    @AllArgsConstructor
    private static class RunContextResponseInterceptor implements HttpResponseInterceptor {
        RunContext runContext;

        @Override
        public void process(HttpResponse response, EntityDetails entity, HttpContext context) throws HttpException, IOException {
            if (context instanceof HttpClientContext httpClientContext &&
                response instanceof BasicClassicHttpResponse httpResponse
            ) {
                try {
                    runContext.logger().debug(
                        "Request '{}' from '{}' with the response code '{}'",
                        httpClientContext.getRequest().getUri(),
                        httpClientContext.getEndpointDetails().getRemoteAddress(),
                        response.getCode()
                    );
                } catch (URISyntaxException e) {
                    throw new IOException(e);
                }

                String[] tags = this.tags(httpClientContext.getRequest(), httpResponse);

                if (httpResponse.getEntity() != null) {
                    runContext.metric(Counter.of(
                        "response.length", httpResponse.getEntity().getContentLength(),
                        tags
                    ));
                }

                runContext.metric(Counter.of("request.count", httpClientContext.getEndpointDetails().getRequestCount(), tags));
                runContext.metric(Counter.of("request.bytes", httpClientContext.getEndpointDetails().getSentBytesCount(), tags));
                runContext.metric(Counter.of("response.bytes", httpClientContext.getEndpointDetails().getReceivedBytesCount(), tags));
                runContext.metric(Counter.of("response.count", httpClientContext.getEndpointDetails().getResponseCount(), tags));
            } else {
                runContext.logger().warn("Invalid response type HttpResponse => {}, HttpContext => {}", response.getClass(), context.getClass());
            }
        }

        protected String[] tags(HttpRequest request, ClassicHttpResponse response) {
            ArrayList<String> tags = new ArrayList<>(Arrays.asList(
                "request.method", request.getMethod(),
                "request.scheme", request.getScheme(),
                "request.hostname", request.getAuthority().getHostName()
            ));

            if (response != null) {
                tags.addAll(
                    Arrays.asList("response.code", String.valueOf(response.getCode()))
                );
            }

            return tags.toArray(String[]::new);
        }
    }

    @AllArgsConstructor
    private static class LoggingResponseInterceptor implements HttpResponseInterceptor {
        Logger logger;
        Level logLevel;

        @Override
        public void process(HttpResponse response, EntityDetails entity, HttpContext context) throws HttpException, IOException {
            String message =  "Response => " +
                buildResponseEntry(response) +
                (logLevel.compareTo(Level.DEBUG) >= 0 ? buildHeadersEntry(response.getHeaders()) : "") +
                (logLevel.compareTo(Level.TRACE) >= 0 ? buildEntityEntry(entity) : "");

            logger.info(message);
        }

        private String buildResponseEntry(HttpResponse response) {
            return "\n  Result => "
                + response.getReasonPhrase();
        }

        private String buildHeadersEntry(Header[] headers) {
            return "\n  Headers => "
                + Arrays.stream(headers)
                .map(header -> header.getName() + ": " + header.getValue())
                .collect(Collectors.joining(", "));
        }

        private String buildEntityEntry(EntityDetails entity) throws IOException {
            if (entity instanceof HttpEntity httpEntity && entity.getContentLength() > 0) {
                if (httpEntity.getContent().markSupported()) {
                    String line = "\n  Payload => " + IOUtils.toString(httpEntity.getContent(), Charsets.UTF_8);
                    httpEntity.getContent().reset();
                    return line;
                } else {
                    return "\n  Payload => (unable to read multiple time)";
                }
            } else {
                return "\n  Payload => (empty body)";
            }
        }
    }

    @AllArgsConstructor
    private static class AllowFailedResponseInterceptor implements HttpResponseInterceptor {
        @Override
        public void process(HttpResponse response, EntityDetails entity, HttpContext context) throws HttpException, IOException {
            if (response.getCode() >= 400) {
                String error = "Failed http request with response " + response.getCode();

                if (entity instanceof HttpEntity httpEntity && entity.getContentLength() > 0) {
                    error += " and body:\n" + IOUtils.toString(httpEntity.getContent(), Charsets.UTF_8);
                }

                throw new HttpClientResponseException(error, response);
            }
        }
    }

    protected CloseableHttpClient client(RunContext runContext) throws IllegalVariableEvaluationException, MalformedURLException, URISyntaxException {
        HttpClientBuilder builder = HttpClients.custom();
        PoolingHttpClientConnectionManagerBuilder connectionManagerBuilder = PoolingHttpClientConnectionManagerBuilder.create();

        if (this.options != null) {
            ConnectionConfig.Builder connectionConfig = ConnectionConfig.custom();

            if (this.options.getConnectTimeout() != null) {
                connectionConfig.setConnectTimeout(Timeout.of(this.options.getConnectTimeout()));
            }

            if (this.options.getReadIdleTimeout() != null) {
                connectionConfig.setSocketTimeout(Timeout.of(this.options.getReadIdleTimeout()));
            }


            if (this.options.getProxyAddress() != null && this.options.getProxyPort() != null) {
                // @TODO use CustomSocketFactory

                if (this.options.getProxyUsername() != null && this.options.getProxyPassword() != null) {
                    builder.setProxyAuthenticationStrategy(new DefaultAuthenticationStrategy());

                    BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                    credentialsProvider.setCredentials(
                        new AuthScope(
                            runContext.render(this.options.getProxyAddress()),
                            this.options.getProxyPort()
                        ),
                        new UsernamePasswordCredentials(
                            runContext.render(this.options.getProxyUsername()),
                            runContext.render(this.options.getProxyPassword()).toCharArray()
                        )
                    );

                    builder.setDefaultCredentialsProvider(credentialsProvider);
                }
            }

            if (this.options.getFollowRedirects() != null && !this.options.getFollowRedirects()) {
                builder.disableRedirectHandling();
            }

            if (this.options.getLogLevel() != null) {
                builder.addRequestInterceptorFirst(new LoggingRequestInterceptor(runContext.logger(), Level.TRACE));
                builder.addResponseInterceptorFirst(new LoggingResponseInterceptor(runContext.logger(), Level.TRACE));
            }

            connectionManagerBuilder.setDefaultConnectionConfig(connectionConfig.build());
        }

        if (this.sslOptions != null) {
            if (this.sslOptions.getInsecureTrustAllCertificates() != null) {
                connectionManagerBuilder.setSSLSocketFactory(this.selfSignedConnectionSocketFactory());
            }
        }

        if (!this.allowFailed) {
            builder.addResponseInterceptorLast(new AllowFailedResponseInterceptor());
        }

        builder.setConnectionManager(connectionManagerBuilder.build());

        builder.addResponseInterceptorFirst(new RunContextResponseInterceptor(runContext));

        return builder.build();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    protected Pair<HttpUriRequest, HttpClientContext> request(RunContext runContext) throws IllegalVariableEvaluationException, URISyntaxException, IOException {
        URI from = new URI(runContext.render(this.uri));

        HttpUriRequest request = new HttpUriRequestBase(this.method.name(), from);

        HttpClientContext localContext = HttpClientContext.create();

        if (this.options != null && this.options.getBasicAuthUser() != null && this.options.getBasicAuthPassword() != null) {
            BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(
                new AuthScope(null, -1),
                new UsernamePasswordCredentials(
                    runContext.render(this.options.getBasicAuthUser()),
                    runContext.render(this.options.getBasicAuthPassword()).toCharArray()
                )
            );
            localContext.setCredentialsProvider(credentialsProvider);
        }

        if (this.formData != null) {
            if (MediaType.MULTIPART_FORM_DATA.equals(this.contentType)) {
                MultipartEntityBuilder builder = MultipartEntityBuilder.create();

                for (Map.Entry<String, Object> e : this.formData.entrySet()) {
                    String key = runContext.render(e.getKey());

                    if (e.getValue() instanceof String stringValue) {
                        String render = runContext.render(stringValue);

                        if (render.startsWith("kestra://")) {
                            File tempFile = runContext.workingDir().createTempFile().toFile();

                            try (OutputStream outputStream = new FileOutputStream(tempFile)) {
                                IOUtils.copyLarge(runContext.storage().getFile(new URI(render)), outputStream);
                            }

                            builder.addPart(key, new FileBody(tempFile));
                        } else {
                            builder.addPart(key, new StringBody(render, ContentType.APPLICATION_OCTET_STREAM));
                        }
                    } else if (e.getValue() instanceof Map mapValue && ((Map<String, String>) mapValue).containsKey("name") && ((Map<String, String>) mapValue).containsKey("content")) {
                        String name = runContext.render(((Map<String, String>) mapValue).get("name"));
                        String content = runContext.render(((Map<String, String>) mapValue).get("content"));

                        File tempFile = runContext.workingDir().createTempFile().toFile();
                        File renamedFile = new File(Files.move(tempFile.toPath(), tempFile.toPath().resolveSibling(name)).toUri());

                        try (OutputStream outputStream = new FileOutputStream(renamedFile)) {
                            IOUtils.copyLarge(runContext.storage().getFile(new URI(content)), outputStream);
                        }

                        builder.addPart(key, new FileBody(renamedFile));
                    } else {
                        builder.addPart(key, new StringBody(JacksonMapper.ofJson().writeValueAsString(e.getValue()), ContentType.APPLICATION_JSON));
                    }
                }

                request.setEntity(builder.build());
            } else {
                request.setEntity(new UrlEncodedFormEntity(runContext.render(this.formData)
                    .entrySet()
                    .stream()
                    .map(e -> new BasicNameValuePair(e.getKey(), e.getValue().toString()))
                    .toList()
                ));
            }
        } else if (this.body != null) {
            request.setEntity(new StringEntity(
                runContext.render(body),
                ContentType.create(
                    runContext.render(this.contentType),
                    this.options != null ? this.options.getDefaultCharset() : StandardCharsets.UTF_8)
                )
            );
        }

        if (this.headers != null) {
            request.setHeaders(this.headers
                .entrySet()
                .stream()
                .map(throwFunction(e -> new BasicHeader(
                    e.getKey().toString(),
                    runContext.render(e.getValue().toString())
                )))
                .toArray(Header[]::new)
            );
        }

        return Pair.of(request, localContext);
    }
}
