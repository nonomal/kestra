package io.kestra.plugin.core.http;

import lombok.Getter;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpResponse;

@Getter
public class HttpClientResponseException extends HttpException {
    HttpResponse response;

    public HttpClientResponseException(String message, HttpResponse response) {
        super(message);
        this.response = response;
    }
}
