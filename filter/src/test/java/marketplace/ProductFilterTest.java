package marketplace;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.common.serialization.*;
import java.util.Properties;
class ProductFilterTest {
    static final String P="""
      {"product_id":"p1","name":"Watch","description":"Demo","category":"Electronics","brand":"A","sku":"A1","store_id":"s1","created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z","price":{"amount":1,"currency":"RUB"},"stock":{"available":1,"reserved":0},"tags":[],"images":[],"specifications":{},"index":"products"}
      """;
    @Test void banBeforeProductAndUnbanAndLateBan() {
        Properties p=new Properties();p.put("application.id","test");p.put("bootstrap.servers","dummy:9092");p.put("statestore.cache.max.bytes",0);
        try(var d=new TopologyTestDriver(ProductFilter.topology(),p)) {
            var products=d.createInputTopic("products.raw",new StringSerializer(),new StringSerializer());
            var bans=d.createInputTopic("products.banned",new StringSerializer(),new StringSerializer());
            var output=d.createOutputTopic("products.filtered",new StringDeserializer(),new StringDeserializer());
            bans.pipeInput("p1","{\"blocked\":true}");products.pipeInput("p1",P);
            assertTrue(output.readValuesToList().stream().allMatch(v->v==null));
            bans.pipeInput("p1","{\"blocked\":false}");assertEquals(P,output.readValue());
            bans.pipeInput("p1","{\"blocked\":true}");assertNull(output.readValue());
            bans.pipeInput("p1",(String)null);assertEquals(P,output.readValue());
        }
    }
    @Test void malformedProductGoesToInvalidTopic() {
        Properties p=new Properties();p.put("application.id","invalid");p.put("bootstrap.servers","dummy:9092");
        try(var d=new TopologyTestDriver(ProductFilter.topology(),p)) {
            var raw=d.createInputTopic("products.raw",new StringSerializer(),new StringSerializer());
            var invalid=d.createOutputTopic("products.invalid",new StringDeserializer(),new StringDeserializer());
            var good=d.createOutputTopic("products.filtered",new StringDeserializer(),new StringDeserializer());
            raw.pipeInput("p1","not-json");raw.pipeInput("wrong-key",P);
            assertEquals(2,invalid.readValuesToList().size());assertTrue(good.isEmpty());
        }
    }
}
