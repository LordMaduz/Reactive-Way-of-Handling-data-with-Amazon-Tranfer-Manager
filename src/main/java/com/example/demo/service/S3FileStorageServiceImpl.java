package com.example.demo.service;

import com.example.demo.configuration.AwsProperties;
import com.example.demo.model.FluxSinkImplementation;
import com.example.demo.model.LoggingTransferListener;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.BlockingOutputStreamAsyncRequestBody;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.zip.GZIPOutputStream;


@RequiredArgsConstructor
@Slf4j
@Service
public class S3FileStorageServiceImpl implements S3FileStorageService {

    private final AwsProperties s3ConfigProperties;
    private final S3TransferManager transferManager;
    private final String S3_BUCKET_FOLDER = "/s3-bucket";
    private final String PUBLISHER_STREAMING_KEY = "stream-directory/Flux.txt";
    private final String BLOCKING_STREAM_STREAMING_KEY = "stream-directory/Flux.tar.gz";
    private final String INPUT_STRING = "This is some test data that will get sent to S3 over and over and " +
            "over again and is about 128 chars long, no seriously... really";

    @Override
    public void uploadObjectToS3(final FilePart filePart, final String path) {

        String filename = filePart.filename();
        String key = path + "/" + filename;

        final File tempFile = new File(System.getProperty("java.io.tmpdir") + filename);

        final PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(s3ConfigProperties.getS3BucketName())
                .key(key)
                .build();

        filePart.transferTo(tempFile).then(
                Mono.fromCallable(() -> {
                    log.info("Size of the File Uploading: {}", tempFile.length());
                    final UploadFileRequest uploadFileRequest = UploadFileRequest.builder()
                            .putObjectRequest(putObjectRequest)
                            .source(tempFile)
                            .addTransferListener(software.amazon.awssdk.transfer.s3.progress.LoggingTransferListener.create())
                            .build();

                    final FileUpload fileUpload = transferManager.uploadFile(uploadFileRequest);
                    Mono.fromFuture(fileUpload.completionFuture()).map(completedFileUpload -> {
                        log.info("File Uploaded Successfully");
                        log.info("Entity Tag of Uploaded File: {}", completedFileUpload.response().eTag());
                        tempFile.deleteOnExit();
                        return Mono.just(true);
                    }).subscribe();
                    return Mono.just(true);
                })).subscribe();
    }


    @Override
    public void uploadStream(final boolean isCompressionEnabled) throws IOException, InterruptedException {
        if (isCompressionEnabled)
            streamWithBlockingOutputStream();
        else
            streamWithPublisher();

    }

    @Override
    public void downloadObjectFromS3(final String key) {

        final GetObjectRequest objectRequest = GetObjectRequest.builder()
                .bucket(s3ConfigProperties.getS3BucketName())
                .key(key)
                .build();

        final String fileName = Arrays.stream(key.split("/")).reduce((x, y) -> y).get();

        final File file = new File(S3_BUCKET_FOLDER.concat(fileName));

        final DownloadFileRequest downloadFileRequest = DownloadFileRequest.builder()
                .getObjectRequest(objectRequest)
                .addTransferListener(software.amazon.awssdk.transfer.s3.progress.LoggingTransferListener.create())
                .destination(file)
                .build();

        final FileDownload downloadFile = transferManager.downloadFile(downloadFileRequest);

        Mono.fromFuture(downloadFile.completionFuture()).subscribe(completedFileDownload -> {
            log.info("File Downloaded Successfully");
            log.info("Content length of File Downloaded [{}]", completedFileDownload.response().contentLength());
        });

    }

    @Override
    public void uploadDirectoryToS3(final Path path, final String bucketPrefix) {

        final UploadDirectoryRequest uploadDirectoryRequest = UploadDirectoryRequest.builder()
                .source(path)
                .s3Delimiter("/")
                .s3Prefix(bucketPrefix.concat("/"))
                .uploadFileRequestTransformer(request -> request.addTransferListener(software.amazon.awssdk.transfer.s3.progress.LoggingTransferListener.create()))
                .bucket(s3ConfigProperties.getS3BucketName())
                .build();

        final DirectoryUpload directoryUpload = transferManager.uploadDirectory(uploadDirectoryRequest);

        Mono.fromFuture(directoryUpload.completionFuture()).subscribe(completedDirectoryUpload -> completedDirectoryUpload.failedTransfers()
                .forEach(failedFileUpload -> log.warn("Object [{}] failed to transfer", failedFileUpload.toString())));

    }

    @Override
    public void downLoadDirectoryFromS3(final Path path, final String bucketPrefix) {

        final DownloadDirectoryRequest downloadDirectoryRequest = DownloadDirectoryRequest.builder()
                .destination(path)
                .listObjectsV2RequestTransformer(l -> l.prefix(bucketPrefix.concat("/")))
                .downloadFileRequestTransformer(request -> request.addTransferListener(software.amazon.awssdk.transfer.s3.progress.LoggingTransferListener.create()))
                .bucket(s3ConfigProperties.getS3BucketName())
                .build();

        final DirectoryDownload directoryDownload = transferManager.downloadDirectory(downloadDirectoryRequest);

        Mono.fromFuture(directoryDownload.completionFuture()).subscribe(completedDirectoryDownload -> completedDirectoryDownload.failedTransfers()
                .forEach(failedFileUpload -> log.warn("Object [{}] failed to transfer", failedFileUpload.toString())));

    }

    private void streamWithPublisher() throws InterruptedException {
        final FluxSinkImplementation<byte[]> fluxSinkImplementation = new FluxSinkImplementation<>();
        final Flux<ByteBuffer> streamFlux = Flux
                .create(fluxSinkImplementation)
                .map(ByteBuffer::wrap)
                .doOnComplete(() -> log.info("Completed"));

        final PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(s3ConfigProperties.getS3BucketName())
                .key(PUBLISHER_STREAMING_KEY)
                .build();

        final UploadRequest uploadRequest = UploadRequest.builder()
                .putObjectRequest(putObjectRequest)
                .requestBody(AsyncRequestBody.fromPublisher(streamFlux))
                .addTransferListener(new LoggingTransferListener(314_572_800L)) //30MB
                .build();

        final Upload upload = transferManager.upload(uploadRequest);

        fluxSinkImplementation.getCountDownLatch().await();

        for (int i = 0; i < 10_000_00; i++) {
            fluxSinkImplementation.publishEvent(INPUT_STRING.getBytes(StandardCharsets.UTF_8));
        }
        fluxSinkImplementation.complete();

        Mono.fromFuture(upload.completionFuture()).subscribe(completedUpload -> {
            log.info("Data Streamed Successfully");
            log.info("Entity Tag of S3 Object Streamed: {}", completedUpload.response().eTag());
        });
    }

    private void streamWithBlockingOutputStream() throws IOException {
        final PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(s3ConfigProperties.getS3BucketName())
                .key(BLOCKING_STREAM_STREAMING_KEY)
                .build();

        final BlockingOutputStreamAsyncRequestBody blockingOutputStreamAsyncRequestBody = AsyncRequestBody.forBlockingOutputStream(null);

        final UploadRequest uploadRequest = UploadRequest.builder()
                .putObjectRequest(putObjectRequest)
                .requestBody(blockingOutputStreamAsyncRequestBody)
                .addTransferListener(new LoggingTransferListener(200L))
                .build();

        final Upload upload = transferManager.upload(uploadRequest);
        final GZIPOutputStream outputStream = new GZIPOutputStream(blockingOutputStreamAsyncRequestBody.outputStream());

        Mono.fromRunnable(() -> {
            for (int i = 0; i < 10_000_000; i++) {
                try {
                    outputStream.write(INPUT_STRING.getBytes(StandardCharsets.UTF_8));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                outputStream.flush();
                outputStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            Mono.fromFuture(upload.completionFuture()).subscribe(completedUpload -> {
                log.info("Data Streamed Successfully");
                log.info("Entity Tag of S3 Object Streamed: {}", completedUpload.response().eTag());
            });
        }).subscribeOn(Schedulers.boundedElastic()).subscribe();
    }
}
