package com.example.demo.model;

import lombok.Getter;
import reactor.core.publisher.FluxSink;

import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;


public class FluxSinkImplementation<T> implements Consumer<FluxSink<T>> {
    private FluxSink<T> fluxSink;
    @Getter
    private final CountDownLatch countDownLatch = new CountDownLatch(1);

    @Override
    public void accept(FluxSink<T> integerFluxSink) {
        this.fluxSink = integerFluxSink;
        countDownLatch.countDown();
    }

    public void publishEvent(T event){
        this.fluxSink.next(event);
    }

    public void complete() {
        fluxSink.complete();
    }
}
