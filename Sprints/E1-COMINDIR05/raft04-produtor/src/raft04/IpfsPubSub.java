package raft04;

import java.io.IOException;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class IpfsPubSub {
    private final String api;
    private final HttpClient http = HttpClient.newHttpClient();

    public IpfsPubSub(String api) {
        this.api = api;
    }

    static String toMultibase(String s) {
        return "u" + Base64.getUrlEncoder().withoutPadding()
                .encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    static String fromMultibase(String s) {
        if (!s.startsWith("u")) throw new IllegalArgumentException("Multibase desconhecido: " + s);
        return new String(Base64.getUrlDecoder().decode(s.substring(1)), StandardCharsets.UTF_8);
    }

    public void publish(String topic, String msg) throws IOException, InterruptedException {
        String url = api + "/pubsub/pub?arg=" + topic + "&arg=" + toMultibase(msg);
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new IOException("pub HTTP " + resp.statusCode() + ": " + resp.body());
        }
    }

    public void subscribe(String topic, Consumer<String> onMessage)
            throws IOException, InterruptedException {
        String url = api + "/pubsub/sub?arg=" + topic;
        HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url)).build();
        HttpResponse<Stream<String>> resp =
                http.send(req, HttpResponse.BodyHandlers.ofLines());
        if (resp.statusCode() != 200) {
            throw new IOException("sub HTTP " + resp.statusCode());
        }
        resp.body().forEach(line -> {
            if (line.isBlank()) return;
            try {
                String data = extractDataField(line);
                if (data != null && !data.isEmpty()) {
                    String payload = fromMultibase(data);
                    onMessage.accept(payload);
                }
            } catch (Exception e) {
                System.err.println("Erro ao processar mensagem do IPFS: " + e.getMessage());
            }
        });
    }

    private static String extractDataField(String json) {
        String needle = "\"data\":\"";
        int i = json.indexOf(needle);
        if (i < 0) return null;
        int start = i + needle.length();
        int end = json.indexOf('"', start);
        if (end < 0) return null;
        return json.substring(start, end);
    }
}
