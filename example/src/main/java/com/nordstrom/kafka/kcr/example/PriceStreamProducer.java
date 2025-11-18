package com.nordstrom.kafka.kcr.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple price streaming producer that generates fake financial market data.
 * Produces JSON messages with ticker symbols, prices, and other market data.
 */
public class PriceStreamProducer {
  private static final Logger log = LoggerFactory.getLogger(PriceStreamProducer.class);
  
  private static final List<String> TICKERS = List.of("AAPL", "MSFT", "AMZN", "GOOGL", "META", "TSLA", "NVDA", "JPM");
  private static final List<String> EXCHANGES = List.of("NYSE", "NASDAQ", "BATS", "IEX");
  private static final int DEFAULT_RATE_MS = 100; // 10 messages per second
  private static final String DEFAULT_TOPIC = "prices";
  private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
  
  private final KafkaProducer<String, String> producer;
  private final String topic;
  private final int rateMs;
  private final Random random;
  private final ObjectMapper mapper;
  private final Map<String, Double> tickerPrices;
  private volatile boolean running = true;

  public PriceStreamProducer(String bootstrapServers, String topic, int rateMs) {
    this.topic = topic;
    this.rateMs = rateMs;
    this.random = new Random();
    this.mapper = new ObjectMapper();
    this.tickerPrices = new HashMap<>();
    
    // Initialize starting prices for each ticker
    tickerPrices.put("AAPL", 175.50);
    tickerPrices.put("MSFT", 380.25);
    tickerPrices.put("AMZN", 155.75);
    tickerPrices.put("GOOGL", 142.30);
    tickerPrices.put("META", 350.60);
    tickerPrices.put("TSLA", 242.80);
    tickerPrices.put("NVDA", 495.20);
    tickerPrices.put("JPM", 155.90);

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.ACKS_CONFIG, "1");
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "price-producer");
    
    this.producer = new KafkaProducer<>(props);
    
    // Shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("Shutting down producer...");
      running = false;
      producer.close();
    }));
  }

  public void start() {
    log.info("Starting price stream producer. Topic: {}, Rate: {} ms", topic, rateMs);
    long messageCount = 0;
    
    while (running) {
      try {
        // Pick a random ticker
        String ticker = TICKERS.get(random.nextInt(TICKERS.size()));
        
        // Generate price movement (small random change)
        double currentPrice = tickerPrices.get(ticker);
        double change = (random.nextDouble() - 0.5) * 2.0; // -1.0 to +1.0
        double newPrice = Math.max(0.01, currentPrice + change);
        tickerPrices.put(ticker, newPrice);
        
        // Create price data
        Map<String, Object> priceData = new HashMap<>();
        priceData.put("ticker", ticker);
        priceData.put("price", Math.round(newPrice * 100.0) / 100.0);
        priceData.put("volume", random.nextInt(1000) + 100);
        priceData.put("exchange", EXCHANGES.get(random.nextInt(EXCHANGES.size())));
        priceData.put("timestamp", Instant.now().toEpochMilli());
        priceData.put("bid", Math.round((newPrice - 0.01) * 100.0) / 100.0);
        priceData.put("ask", Math.round((newPrice + 0.01) * 100.0) / 100.0);
        
        String jsonValue = mapper.writeValueAsString(priceData);
        
        ProducerRecord<String, String> record = new ProducerRecord<>(
            topic,
            ticker,  // Use ticker as key for partitioning
            jsonValue
        );
        
        producer.send(record, (metadata, exception) -> {
          if (exception != null) {
            log.error("Error sending message", exception);
          } else {
            log.debug("Sent: {} to partition {} at offset {}", 
                ticker, metadata.partition(), metadata.offset());
          }
        });
        
        messageCount++;
        if (messageCount % 100 == 0) {
          log.info("Sent {} messages", messageCount);
        }
        
        Thread.sleep(rateMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        log.error("Error in producer loop", e);
      }
    }
    
    log.info("Producer stopped. Total messages sent: {}", messageCount);
  }

  public static void main(String[] args) {
    String bootstrapServers = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS);
    String topic = System.getenv().getOrDefault("TOPIC", DEFAULT_TOPIC);
    int rateMs = Integer.parseInt(System.getenv().getOrDefault("RATE_MS", String.valueOf(DEFAULT_RATE_MS)));
    
    log.info("Price Stream Producer Configuration:");
    log.info("  Bootstrap Servers: {}", bootstrapServers);
    log.info("  Topic: {}", topic);
    log.info("  Rate: {} ms ({} msg/sec)", rateMs, 1000 / rateMs);
    
    PriceStreamProducer producer = new PriceStreamProducer(bootstrapServers, topic, rateMs);
    producer.start();
  }
}
