package demo;

import entity.User;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.search.FTCreateParams;
import redis.clients.jedis.search.IndexDataType;
import redis.clients.jedis.search.Query;
import redis.clients.jedis.search.aggr.AggregationBuilder;
import redis.clients.jedis.search.aggr.AggregationResult;
import redis.clients.jedis.search.aggr.Reducers;
import redis.clients.jedis.search.schemafields.NumericField;
import redis.clients.jedis.search.schemafields.TagField;
import redis.clients.jedis.search.schemafields.TextField;

public class Main {
    public static void main(String[] args) {
        JedisPooled jedis = new JedisPooled("localhost", 6379);
        User user1 = new User("Paul John", "paul.john@example.com", 42, "London");
        User user2 = new User("Eden Zamir", "eden.zamir@example.com", 29, "Tel Aviv");
        User user3 = new User("Paul Zamir", "paul.zamir@example.com", 35, "Tel Aviv");

//        jedis.ftCreate("idx:users",
//                FTCreateParams.createParams()
//                        .on(IndexDataType.JSON)
//                        .addPrefix("user:"),
//                TextField.of("$.name").as("name"),
//                TextField.of("$.email").as("email"),
//                TagField.of("$.city").as("city"),
//                NumericField.of("$.age").as("age")
//        );

        jedis.jsonSetWithEscape("user:1", user1);
        jedis.jsonSetWithEscape("user:2", user2);
        jedis.jsonSetWithEscape("user:3", user3);

        var query = new Query("Paul @age:[30 40]");
        var result = jedis.ftSearch("idx:users", query).getDocuments();
        System.out.println(result);
        // Prints: [id:user:3, score: 1.0, payload:null, properties:[$={"name":"Paul Zamir","email":"paul.zamir@example.com","age":35,"city":"Tel Aviv"}]]

        var city_query = new Query("Paul @age:[30 40]");
        var city_result = jedis.ftSearch("idx:users", city_query.returnFields("city")).getDocuments();
        System.out.println(city_result);
        // Prints: [id:user:3, score: 1.0, payload:null, properties:[city=Tel Aviv]]

        AggregationBuilder ab = new AggregationBuilder("*")
                .groupBy("@city", Reducers.count().as("count"));
        AggregationResult ar = jedis.ftAggregate("idx:users", ab);

        for (int idx=0; idx < ar.getTotalResults(); idx++) {
            System.out.println(ar.getRow(idx).getString("city") + " - " + ar.getRow(idx).getString("count"));
        }
        // Prints:
        // London - 1
        // Tel Aviv - 2
    }
}
