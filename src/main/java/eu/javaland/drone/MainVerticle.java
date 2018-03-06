package eu.javaland.drone;

import com.uber.jaeger.Configuration;
import com.uber.jaeger.samplers.ConstSampler;
import io.opentracing.Scope;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMapExtractAdapter;
import io.opentracing.propagation.TextMapInjectAdapter;
import io.opentracing.tag.Tags;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.WebSocket;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class MainVerticle extends AbstractVerticle {
    private static final Logger logger = LoggerFactory.getLogger(MainVerticle.class);
    private static final Tracer tracer = new Configuration("javaland-vertx-drone")
            .withReporter(
                    new Configuration.ReporterConfiguration()
                            .withLogSpans(true)
            )
            .withSampler(
                    new Configuration.SamplerConfiguration()
                            .withType(ConstSampler.TYPE)
                            .withParam(1)
            )
            .getTracerBuilder()
            .build();

    public static void main(String[] args) {
        logger.warn("Bootstrapping from the main method. For production purposes, use the Vert.x launcher");
        Vertx.vertx().deployVerticle(new MainVerticle());
    }

    @Override
    public void start() {
        vertx.createHttpClient().websocket(8090, "localhost" , "/socket", socket -> {
            socket.handler(this::messageHandler);
            socket.closeHandler(this::closeHandler);

            try (Scope ignored = tracer.buildSpan("create-drone").startActive(true)) {
                register(socket);
            }

            logger.info("Connected to the server");
        });
    }

    private void register(WebSocket socket) {
        String id = UUID.randomUUID().toString();
        logger.info("Hello, I'm the drone {}", id);

        tracer.activeSpan().setTag("id", id);
        tracer.activeSpan().setTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT);

        Map<String, String> jsonObject = new HashMap<>(4);
        jsonObject.put("action", "register");
        jsonObject.put("id", id);
        jsonObject.put("lat", "48.133333");
        jsonObject.put("lon", "11.566667");

        tracer.inject(tracer.activeSpan().context(), Format.Builtin.TEXT_MAP, new TextMapInjectAdapter(jsonObject));
        socket.writeTextMessage(new JsonObject(new HashMap<>(jsonObject)).toString());
    }

    private void closeHandler(Void handler) {
        logger.info("Socket has been closed. I have no reasons to keep running.");
        vertx.close();
    }

    private void messageHandler(Buffer handler) {
        JsonObject request = handler.toJsonObject();
        logger.info("Got message: {}", request.toString());

        Map<String, String> converted = request.getMap().entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
        SpanContext context = tracer.extract(Format.Builtin.TEXT_MAP, new TextMapExtractAdapter(converted));

        try (Scope scope = tracer
                .buildSpan("move-to-location")
                .asChildOf(context)
                .startActive(true)) {
            logger.info("On my way!");
            try {
                long wait = (long)(Math.random() * 1000);
                scope.span().setTag("wait", wait);
                Thread.sleep(wait);
            } catch (InterruptedException ignored) {
            }
        }
    }

}
