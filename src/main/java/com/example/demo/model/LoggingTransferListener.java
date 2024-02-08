package com.example.demo.model;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.transfer.s3.progress.TransferListener;

@Slf4j
@RequiredArgsConstructor
public class LoggingTransferListener implements TransferListener {

    private final long byteSize;
    private long startTime;
    private int step=0;
    @Override
    public void transferInitiated (Context.TransferInitiated context) {
        log.info("Transfer initiated: {}", context.progressSnapshot().ratioTransferred());
        startTime = System.currentTimeMillis();
        logTransferStatus(context.progressSnapshot().transferredBytes());
    }

    private void logTransferStatus(long l) {
        if (l > step * byteSize) { //5MB
            log.info("Bytes transferred: {} ", l);
            step++;
        }
    }

    @Override
    public void bytesTransferred (Context.BytesTransferred context) {
        logTransferStatus(context.progressSnapshot().transferredBytes());
    }

    @Override
    public void transferComplete (Context.TransferComplete context) {
        long seconds = (System.currentTimeMillis() - startTime) / 1000;
        double bytes = (double)context.progressSnapshot().transferredBytes();
        double megabytes = bytes / 1_048_576;
        double throughput = megabytes / seconds;
        log.info("Transfer complete: \n\t Bytes Transferred: {}\n\t MBs: {}\n\t Total Time Taken: {} Seconds\n\t Throughput: {} MB/s",
                String.format("%10f", bytes), String.format("%.3f", megabytes),
                seconds,
                String.format("%.2f", throughput));
    }

    @Override
    public void transferFailed (Context.TransferFailed context) {
        log.error("Transfer failed on resource ", context.exception());
    }
}