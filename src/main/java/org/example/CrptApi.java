package org.example;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CrptApi {

    public static void main(String[] args) throws IOException, InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(6);
        Product product = new Product(
                "cert_doc", "2023-09-12", "123456",
                "9876543210", "1111111111", "2023-09-12",
                "TNVED123", "UIT123", "UITU123"
        );

        Document document = new Document(
                "1234567890", "doc123", "NEW", true,
                "9876543210", "1111111111", "2023-09-12",
                "type1", product, "2023-09-12", "REG123"
        );
        CrptApi crptApi = new CrptApi(TimeUnit.MINUTES, 5);
        for (int i = 1; i <= 20; i++) {
            executorService.submit(() -> {
                try {
                    crptApi.createDocument(document, "1");
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        executorService.shutdown();
    }

    private final long timeUnit;
    private final int requestLimit;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final Semaphore semaphore;
    private final ScheduledExecutorService scheduler;
    private final AtomicInteger currentRequests;

    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        this.timeUnit = timeUnit.toMillis(1);
        if (requestLimit <= 0) throw new IllegalArgumentException("Request Limit cannot be less than or equal to 0");
        else this.requestLimit = requestLimit;
        this.objectMapper = new ObjectMapper();
        this.semaphore = new Semaphore(requestLimit);
        this.httpClient = HttpClient.newHttpClient();
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.currentRequests = new AtomicInteger(0);

        scheduler.scheduleAtFixedRate(
                this::resetCurrentLimit,
                0,
                this.timeUnit,
                TimeUnit.MILLISECONDS);
    }

    public void createDocument(Document document, String signature) throws InterruptedException, IOException {
        try {
            semaphore.acquire();

            if (currentRequests.get() >= requestLimit) {
                waitFreeRequest();
            }
            currentRequests.incrementAndGet();
            String json = objectMapper.writeValueAsString(document);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("https://ismp.crpt.ru/api/v3/lk/documents/create"))
                    .header("Content-Type", "application/json")
                    .header("Signature", signature)
                    .POST(HttpRequest.BodyPublishers.ofString(json))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new RuntimeException("Request error: " + response.statusCode());
            }

        } catch (JsonProcessingException | InterruptedException e) {
            throw e;
        } finally {
            semaphore.release();
        }
    }

    private void waitFreeRequest() throws InterruptedException {
        while (currentRequests.get() >= requestLimit) Thread.sleep(100);
    }

    private void resetCurrentLimit() {
        currentRequests.set(0);
    }

    public void shutdown() {
        scheduler.shutdown();
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @ToString
    public static class Document {
        @JsonProperty("participantInn")
        private String participantInn;

        @JsonProperty("doc_id")
        private String docId;

        @JsonProperty("doc_status")
        private String docStatus;

        @JsonProperty("importRequest")
        private boolean importRequest;

        @JsonProperty("owner_inn")
        private String ownerInn;

        @JsonProperty("producer_inn")
        private String producerInn;

        @JsonProperty("production_date")
        private String productionDate;

        @JsonProperty("production_type")
        private String productionType;

        @JsonProperty("products")
        private Product product;

        @JsonProperty("reg_date")
        private String regDate;

        @JsonProperty("reg_number")
        private String regNumber;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @ToString
    public static class Product {
        @JsonProperty("certificate_document")
        private String certificateDocument;

        @JsonProperty("certificate_document_date")
        private String certificateDocumentDate;

        @JsonProperty("certificate_document_number")
        private String certificateDocumentNumber;

        @JsonProperty("owner_inn")
        private String ownerInn;

        @JsonProperty("producer_inn")
        private String producerInn;

        @JsonProperty("production_date")
        private String productionDate;

        @JsonProperty("tnved_code")
        private String tnvedCode;

        @JsonProperty("uit_code")
        private String uitCode;

        @JsonProperty("uitu_code")
        private String uituCode;
    }
}
