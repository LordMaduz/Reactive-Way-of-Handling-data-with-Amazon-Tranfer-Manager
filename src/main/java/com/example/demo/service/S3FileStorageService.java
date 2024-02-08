package com.example.demo.service;

import org.springframework.http.codec.multipart.FilePart;
import java.io.IOException;
import java.nio.file.Path;

public interface S3FileStorageService {



    void uploadObjectToS3(final FilePart filePart, final String path);

    void uploadStream(final boolean isCompressionEnabled) throws InterruptedException, IOException;

    void downloadObjectFromS3(final String key);

    void uploadDirectoryToS3(final Path path, final String bucketPrefix);

    void downLoadDirectoryFromS3(final Path path, String bucketPrefix);

}
