# AWS S3 Transfer Manager with Spring Boot

> High-performance file transfer solution using AWS S3 Transfer Manager for parallel uploads/downloads with streaming support and directory synchronization.

## Overview

A production-ready Spring Boot application demonstrating advanced AWS S3 operations using the S3 Transfer Manager. Features asynchronous parallel transfers, reactive streaming, compression support, and efficient handling of large files and directories with progress tracking.

**Key Features:**
- Parallel multipart uploads/downloads with S3 Transfer Manager
- Reactive streaming to S3 with backpressure
- Directory upload/download with batch operations
- Real-time transfer progress monitoring
- GZIP compression for streaming data
- Asynchronous non-blocking operations
- Configurable chunk sizes and parallelism
- OpenAPI/Swagger documentation

---

## Architecture
<img width="2602" height="1901" alt="Mermaid Chart - Create complex, visual diagrams with text -2025-10-14-115827" src="https://github.com/user-attachments/assets/2b9bd77c-1962-424e-8b59-025605f6f6f6" />

---

## Transfer Flow
### Multipart Upload Flow
<img width="1481" height="1802" alt="Mermaid Chart - Create complex, visual diagrams with text -2025-10-14-115618" src="https://github.com/user-attachments/assets/3449cc26-7639-4eef-b2e0-8b06845dc0e3" />

## Streaming Flow
<img width="1751" height="1472" alt="Mermaid Chart - Create complex, visual diagrams with text -2025-10-14-115741" src="https://github.com/user-attachments/assets/b3a865a0-c8fa-4b36-9165-7c3f5dc6539c" />

## Tech Stack

| Category | Technologies |
|----------|-------------|
| **Core** | Java 21, Spring Boot 3.2.2 |
| **Reactive** | Spring WebFlux, Project Reactor |
| **AWS** | AWS SDK for Java 2.23.14 |
| **Transfer** | S3 Transfer Manager, AWS CRT 0.29.9 |
| **Compression** | GZIP |
| **API Docs** | SpringDoc OpenAPI 3 |

---

## Getting Started

### Prerequisites
```bash
- Java 21+
- Maven 3.8+
- AWS Account with S3 bucket
- AWS credentials configured
```

### AWS Configuration
Option 1: Environment Variables

```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_REGION=us-east-1
```

Option 2: Application Configuration
application.yml:
```yaml
aws:
  access-key: your-access-key
  secret-key: your-secret-key
  region: us-east-1
  s3-bucket-name: your-bucket-name
  multipart-min-part-size: 5242880  # 5MB
```
### Installation

1. Build Application
```bash
git clone <repository-url>
cd s3-transfer-manager
mvn clean install
```
2. Run Application
```bash
mvn spring-boot:run
```
Application starts on http://localhost:8080
Swagger UI: http://localhost:8080/webjars/swagger-ui/index.html

### API Usage
#### Upload File with Transfer Manager

Request:
```bash
curl -X POST http://localhost:8080/files/upload-transfer-manager/documents \
  -F "file=@/path/to/largefile.zip"
```
Response:
```
SUCCESS
```

#### What Happens:

- File automatically split into parts (5MB each)
- Parts uploaded in parallel
- Progress tracked in real-time
- Optimized for large files (GB+)

#### Stream Data to S3
Request (Uncompressed):
```bash
curl -X POST http://localhost:8080/files/upload-stream/false
```

Request (GZIP Compressed):
```bash
curl -X POST http://localhost:8080/files/upload-stream/true
```

#### What Happens:

- Data streamed directly to S3 without disk buffering
- Compression applied on-the-fly (if enabled)
- Memory-efficient for continuous data streams

#### Download File

Request:
```bash
curl -X GET http://localhost:8080/files/download-transfer-manager/documents/largefile.zip
```

Response:
```
SUCCESS
```

#### What Happens:
 - File downloaded in parallel chunks
 - Progress tracked
 - Optimized throughput

#### Upload Directory

Request:
```bash
curl -X POST http://localhost:8080/files/upload-directory/backups/local-dir
```

#### What Happens:
 - All files in directory uploaded recursively
 - Maintains directory structure
 - Failed transfers reported

#### Download Directory

Request:
```bash
curl -X POST http://localhost:8080/files/download-directory/backups/restore-dir
```
## Key Features

### S3 Transfer Manager Configuration
```java
@Configuration
@RequiredArgsConstructor
public class AwsS3Config {

    @Bean
    public S3AsyncClient s3AsyncClientCrt(AwsCredentialsProvider credentials) {
        return S3AsyncClient.crtBuilder()
            .minimumPartSizeInBytes(s3ConfigProperties.getMultipartMinPartSize())
            .credentialsProvider(credentials)
            .build();
    }

    @Bean
    public S3TransferManager transferManager(S3AsyncClient s3AsyncClient) {
        return S3TransferManager.builder()
            .s3Client(s3AsyncClient)
            .build();
    }
}
```

### File Upload with Progress Tracking
```java
public void uploadObjectToS3(FilePart filePart, String path) {
   
    PutObjectRequest putObjectRequest = PutObjectRequest.builder()
        .bucket(s3ConfigProperties.getS3BucketName())
        .key(key)
        .build();

    filePart.transferTo(tempFile).then(
        Mono.fromCallable(() -> {
            UploadFileRequest uploadFileRequest = UploadFileRequest.builder()
                .putObjectRequest(putObjectRequest)
                .source(tempFile)
                .addTransferListener(LoggingTransferListener.create())
                .build();

            FileUpload fileUpload = transferManager.uploadFile(uploadFileRequest);
            
            Mono.fromFuture(fileUpload.completionFuture())
                .subscribe(completed -> {
                    log.info("File uploaded: {}", completed.response().eTag());
                    tempFile.deleteOnExit();
                });
            
            return true;
        })
    ).subscribe();
}
```

### Streaming with Flux Publisher
```java
private void streamWithPublisher() throws InterruptedException {
    FluxSinkImplementation fluxSink = new FluxSinkImplementation<>();
    
    Flux streamFlux = Flux
        .create(fluxSink)
        .map(ByteBuffer::wrap)
        .doOnComplete(() -> log.info("Stream completed"));

    PutObjectRequest putObjectRequest = PutObjectRequest.builder()
        .bucket(s3ConfigProperties.getS3BucketName())
        .key("stream-directory/data.txt")
        .build();

    UploadRequest uploadRequest = UploadRequest.builder()
        .putObjectRequest(putObjectRequest)
        .requestBody(AsyncRequestBody.fromPublisher(streamFlux))
        .addTransferListener(new LoggingTransferListener(5_242_880L))
        .build();

    Upload upload = transferManager.upload(uploadRequest);

    // Wait for sink to be ready
    fluxSink.getCountDownLatch().await();

    // Stream data
    for (int i = 0; i < 1_000_000; i++) {
        fluxSink.publishEvent(DATA.getBytes(StandardCharsets.UTF_8));
    }
    fluxSink.complete();

    Mono.fromFuture(upload.completionFuture())
        .subscribe(completed -> 
            log.info("Streamed: {}", completed.response().eTag()));
}
```

### Streaming with Compression
```java
private void streamWithBlockingOutputStream() throws IOException {
    BlockingOutputStreamAsyncRequestBody requestBody = 
        AsyncRequestBody.forBlockingOutputStream(null);

    UploadRequest uploadRequest = UploadRequest.builder()
        .putObjectRequest(putObjectRequest)
        .requestBody(requestBody)
        .addTransferListener(new LoggingTransferListener(200L))
        .build();

    Upload upload = transferManager.upload(uploadRequest);
    
    GZIPOutputStream gzipStream = 
        new GZIPOutputStream(requestBody.outputStream());

    Mono.fromRunnable(() -> {
        try {
            for (int i = 0; i < 10_000_000; i++) {
                gzipStream.write(DATA.getBytes(StandardCharsets.UTF_8));
            }
            gzipStream.flush();
            gzipStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Mono.fromFuture(upload.completionFuture())
            .subscribe(completed -> 
                log.info("Compressed stream uploaded: {}", completed.response().eTag()));
    }).subscribeOn(Schedulers.boundedElastic()).subscribe();
}
```

### Directory Upload
```java
public void uploadDirectoryToS3(Path path, String bucketPrefix) {
    UploadDirectoryRequest uploadRequest = UploadDirectoryRequest.builder()
        .source(path)
        .s3Delimiter("/")
        .s3Prefix(bucketPrefix + "/")
        .uploadFileRequestTransformer(request -> 
            request.addTransferListener(LoggingTransferListener.create()))
        .bucket(s3ConfigProperties.getS3BucketName())
        .build();

    DirectoryUpload directoryUpload = transferManager.uploadDirectory(uploadRequest);

    Mono.fromFuture(directoryUpload.completionFuture())
        .subscribe(completed -> 
            completed.failedTransfers().forEach(failed -> 
                log.warn("Failed to upload: {}", failed.toString())));
}
```

## Learn More

For comprehensive guides on AWS S3 Transfer Manager and advanced streaming techniques:

### Complete Implementation Series

**AWS S3 Transfer Optimization Tutorials:**

#### 1. Parallel File Transfers

**[Asynchronous Parallel File Transfers with S3 Transfer Manager](https://levelup.gitconnected.com/asynchronous-parallel-file-transfers-using-amazon-s3-transfer-manager-0da7fa5ff83f?sk=eb5b28271241a40e2c90a1c2f92b12ab)**


#### 2. Streaming Data to S3

**[Streaming Data to Amazon S3 Asynchronously](https://levelup.gitconnected.com/asynchronous-streaming-of-data-to-amazon-s3-c4f8e066fa9b?sk=f25af74e909da088226f3be543741f97)**

  
  

