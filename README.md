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

---

## Transfer Flow
### Multipart Upload Flow
<img width="1481" height="1802" alt="Mermaid Chart - Create complex, visual diagrams with text -2025-10-14-115618" src="https://github.com/user-attachments/assets/3449cc26-7639-4eef-b2e0-8b06845dc0e3" />


## Streaming Flow

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

#### Please take a look at the below articles for a detail explanation of the above intergrations.

* [Asynchronous Parallel File Transfers with S3 Transfer Manager](https://levelup.gitconnected.com/asynchronous-parallel-file-transfers-using-amazon-s3-transfer-manager-0da7fa5ff83f?sk=eb5b28271241a40e2c90a1c2f92b12ab)
* [Streaming data to Amazon S3 Asynchronously](https://levelup.gitconnected.com/asynchronous-streaming-of-data-to-amazon-s3-c4f8e066fa9b?sk=f25af74e909da088226f3be543741f97)

  
  

