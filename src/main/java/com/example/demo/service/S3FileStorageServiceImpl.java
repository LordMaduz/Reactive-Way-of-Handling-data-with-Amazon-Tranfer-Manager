package com.example.demo.service;

import com.example.demo.configuration.AwsProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.*;
import software.amazon.awssdk.transfer.s3.progress.LoggingTransferListener;
import java.io.*;
import java.nio.file.Path;
import java.util.*;


@RequiredArgsConstructor
@Slf4j
@Service
public class S3FileStorageServiceImpl implements S3FileStorageService {

    private final AwsProperties s3ConfigProperties;
    private final S3TransferManager transferManager;
    private final String S3_BUCKET_FOLDER = "s3-bucket/";

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
                            .addTransferListener(LoggingTransferListener.create())
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
    public void downloadObjectFromS3(final String key) {

        final GetObjectRequest objectRequest = GetObjectRequest.builder()
                .bucket(s3ConfigProperties.getS3BucketName())
                .key(key)
                .build();

        final String fileName = Arrays.stream(key.split("/")).reduce((x, y) -> y).get();

        final File file = new File(S3_BUCKET_FOLDER.concat(fileName));

        final DownloadFileRequest downloadFileRequest = DownloadFileRequest.builder()
                .getObjectRequest(objectRequest)
                .addTransferListener(LoggingTransferListener.create())
                .destination(file)
                .build();

        final FileDownload downloadFile = transferManager.downloadFile(downloadFileRequest);

        Mono.fromFuture(downloadFile.completionFuture()).subscribe(completedFileDownload -> {
            log.info("File Downloaded Successfully");
            log.info("Content length of File Downloaded [{}]", completedFileDownload.response().contentLength());
        });

    }

    @Override
    public void uploadDirectoryToS3(final Path path, final String bucketPrefix ) {

        final UploadDirectoryRequest uploadDirectoryRequest = UploadDirectoryRequest.builder()
                .source(path)
                .s3Delimiter("/")
                .s3Prefix(bucketPrefix.concat("/"))
                .uploadFileRequestTransformer(request -> request.addTransferListener(LoggingTransferListener.create()))
                .bucket(s3ConfigProperties.getS3BucketName())
                .build();

        final DirectoryUpload directoryUpload = transferManager.uploadDirectory(uploadDirectoryRequest);

        Mono.fromFuture(directoryUpload.completionFuture()).subscribe(completedDirectoryUpload -> {
            completedDirectoryUpload.failedTransfers()
                    .forEach(failedFileUpload -> log.warn("Object [{}] failed to transfer", failedFileUpload.toString()));
        });

    }

    @Override
    public void downLoadDirectoryFromS3(final Path path, final String bucketPrefix) {

        final DownloadDirectoryRequest downloadDirectoryRequest = DownloadDirectoryRequest.builder()
                .destination(path)
                .listObjectsV2RequestTransformer(l -> l.prefix(bucketPrefix.concat("/")))
                .downloadFileRequestTransformer(request -> request.addTransferListener(LoggingTransferListener.create()))
                .bucket(s3ConfigProperties.getS3BucketName())
                .build();

        final DirectoryDownload directoryDownload = transferManager.downloadDirectory(downloadDirectoryRequest);

        Mono.fromFuture(directoryDownload.completionFuture()).subscribe(completedDirectoryDownload -> {
            completedDirectoryDownload.failedTransfers()
                    .forEach(failedFileUpload -> log.warn("Object [{}] failed to transfer", failedFileUpload.toString()));
        });

    }
}
