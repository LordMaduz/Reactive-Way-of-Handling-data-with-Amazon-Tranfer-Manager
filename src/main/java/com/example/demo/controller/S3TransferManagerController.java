package com.example.demo.controller;


import com.example.demo.service.S3FileStorageService;
import lombok.*;
import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;
import java.io.IOException;
import java.nio.file.Path;

@RequiredArgsConstructor
@RestController
@RequestMapping("/files")
@Validated
public class S3TransferManagerController {

    private final S3FileStorageService fileStorageService;

    @PostMapping(value = "/upload-transfer-manager/{path}", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Mono<String> uploadUsingTransferManager(@RequestPart("file") Mono<FilePart> filePartMono, @PathVariable String path) {

        return filePartMono
                .flatMap(file -> {
                    fileStorageService.uploadObjectToS3(file, path);
                    return Mono.just("SUCCESS");
                });


    }

    @PostMapping(value = "/upload-stream/{compressionEnabled}")
    public Mono<String> uploadStream(@PathVariable("compressionEnabled") boolean compressionEnabled) throws InterruptedException, IOException {
        fileStorageService.uploadStream(compressionEnabled);
        return Mono.just("SUCCESS");
    }

    @GetMapping(path = "download-transfer-manager/{fileKey}")
    public Mono<String> downloadUsingTransferManager(@PathVariable("fileKey") String fileKey) {

        fileStorageService.downloadObjectFromS3(fileKey);
        return Mono.just("SUCCESS");
    }

    @PostMapping(path = "upload-directory/{path}/{prefix}")
    public Mono<String> uploadDirectoryToS3(@PathVariable("prefix") String prefix, @PathVariable("path") String path) {

        fileStorageService.uploadDirectoryToS3(Path.of(path), prefix);
        return Mono.just("SUCCESS");
    }

    @PostMapping(path = "download-directory/{path}/{prefix}")
    public Mono<String> downloadDirectoryFromS3(@PathVariable("prefix") String prefix, @PathVariable("path") String path) {

        fileStorageService.downLoadDirectoryFromS3(Path.of(path), prefix);
        return Mono.just("SUCCESS");
    }
}
