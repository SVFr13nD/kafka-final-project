package marketplace;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class ProductFilter {
    static final ObjectMapper JSON = new ObjectMapper();
    static JsonNode parse(String value) {
        try { return JSON.readTree(value); } catch (Exception e) { return null; }
    }
    static boolean valid(String key, String value) {
        if (key == null || value == null) return false;
        JsonNode p = parse(value);
        if (p == null || !p.isObject() || !key.equals(p.path("product_id").asText())) return false;
        for (String field : List.of("product_id","name","description","category","brand","sku","store_id","created_at","updated_at")) {
            if (!p.path(field).isTextual() || p.path(field).asText().isBlank()) return false;
        }
        JsonNode amount = p.path("price").path("amount");
        if (!amount.isNumber() || amount.asDouble() < 0 || !p.path("price").path("currency").asText().matches("[A-Z]{3}")) return false;
        for (String field : List.of("available","reserved")) {
            JsonNode n=p.path("stock").path(field);
            if (!n.isIntegralNumber() || n.asLong()<0) return false;
        }
        return p.path("tags").isArray() && p.path("images").isArray() && p.path("specifications").isObject() && "products".equals(p.path("index").asText());
    }
    static boolean blocked(String ban) {
        if (ban == null) return false;
        JsonNode parsed=parse(ban);
        // Invalid moderation state fails closed for that product.
        return parsed == null || !parsed.path("blocked").isBoolean() || parsed.path("blocked").asBoolean();
    }
    public static Topology topology() {
        StreamsBuilder b = new StreamsBuilder();
        var strings=Consumed.with(Serdes.String(),Serdes.String());
        KStream<String,String> raw=b.stream("products.raw",strings);
        raw.filter((k,v)->!valid(k,v)).to("products.invalid",Produced.with(Serdes.String(),Serdes.String()));
        KTable<String,String> products=raw.filter(ProductFilter::valid).toTable(
            Materialized.<String,String,KeyValueStore<org.apache.kafka.common.utils.Bytes,byte[]>>as("products-store")
                .withKeySerde(Serdes.String()).withValueSerde(Serdes.String()));
        KTable<String,String> bans=b.table("products.banned",strings,
            Materialized.<String,String,KeyValueStore<org.apache.kafka.common.utils.Bytes,byte[]>>as("bans-store")
                .withKeySerde(Serdes.String()).withValueSerde(Serdes.String()));
        // A table-table join reacts to product AND ban changes: a late ban emits a tombstone.
        products.leftJoin(bans,(product,ban)->blocked(ban)?null:product)
                .toStream().to("products.filtered",Produced.with(Serdes.String(),Serdes.String()));
        return b.build();
    }
    public static void main(String[] args) throws Exception {
        Properties p=new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG,"product-filter");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"primary-1:9092,primary-2:9092,primary-3:9092");
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.StringSerde.class.getName());
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.StringSerde.class.getName());
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,StreamsConfig.EXACTLY_ONCE_V2);
        p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG,3);
        p.put(StreamsConfig.STATE_DIR_CONFIG,"/state");
        p.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG,0);
        p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,100);
        p.put("security.protocol","SSL");
        p.put("ssl.truststore.type","PKCS12");p.put("ssl.truststore.location","/certs/truststore.p12");
        p.put("ssl.truststore.password","changeit");
        p.put("ssl.keystore.type","PKCS12");p.put("ssl.keystore.location","/certs/filter.p12");
        p.put("ssl.keystore.password","changeit");p.put("ssl.key.password","changeit");
        p.put("ssl.endpoint.identification.algorithm","https");
        KafkaStreams streams=new KafkaStreams(topology(),p);
        streams.setUncaughtExceptionHandler(e->{e.printStackTrace();return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;});
        HttpServer server=HttpServer.create(new InetSocketAddress(8090),0);
        server.createContext("/health",x->{
            boolean ready=streams.state()==KafkaStreams.State.RUNNING;
            byte[] bytes=(ready?"RUNNING":streams.state().toString()).getBytes(StandardCharsets.UTF_8);
            x.sendResponseHeaders(ready?200:503,bytes.length);x.getResponseBody().write(bytes);x.close();
        });
        server.createContext("/bans",x->{
            try {
                ReadOnlyKeyValueStore<String,String> store=streams.store(StoreQueryParameters.fromNameAndType("bans-store",QueryableStoreTypes.keyValueStore()));
                Map<String,Boolean> state=new TreeMap<>();
                try(KeyValueIterator<String,String> it=store.all()) {while(it.hasNext()){var v=it.next();state.put(v.key,blocked(v.value));}}
                byte[] bytes=JSON.writeValueAsBytes(state);x.getResponseHeaders().set("Content-Type","application/json");
                x.sendResponseHeaders(200,bytes.length);x.getResponseBody().write(bytes);
            } catch(Exception e){x.sendResponseHeaders(503,-1);} finally{x.close();}
        });
        Runtime.getRuntime().addShutdownHook(new Thread(()->{server.stop(0);streams.close();}));
        streams.start();server.start();
    }
}
