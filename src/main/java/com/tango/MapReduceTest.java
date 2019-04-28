package com.tango;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class MapReduceTest {

    public static void main(String[] args) throws InterruptedException {
        Mono<String> input = Mono.just("__data__");

        System.out.println(format("Main thread is %s", Thread.currentThread().getName()));

        //Split to 2 parallel streams
        Mono<Integer> firstWork =
                input
                        .subscribeOn(Schedulers.newSingle("First job thread"))
                        .map(String::length)
                .doOnEach(integerSignal -> {
                            if (!integerSignal.isOnNext()) {
                                return;
                            }
                            System.out.println(format(
                                    "Thread: %s, START signal: %s, data: %s",
                                    Thread.currentThread().getName(),
                                    integerSignal.getType(),
                                    integerSignal.get()));
                            try {
                                TimeUnit.SECONDS.sleep(4);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            System.out.println(format(
                                    "Thread: %s, STOP signal: %s, data: %s",
                                    Thread.currentThread().getName(),
                                    integerSignal.getType(),
                                    integerSignal.get()));
                        }
                );

        Mono<String> secondWork =
                Mono.defer(() -> input)
        //Mono.just("sdsds")
                        .subscribeOn(Schedulers.newSingle("Second job tread"))
                        .map(s -> s.concat("!!!!"))
                        .doOnEach(integerSignal -> {
                                    if (!integerSignal.isOnNext()) {
                                        return;
                                    }
                                    System.out.println(format(
                                            "Thread: %s, START signal: %s, data: %s",
                                            Thread.currentThread().getName(),
                                            integerSignal.getType(),
                                            integerSignal.get()));
                                    try {
                                        TimeUnit.SECONDS.sleep(2);
                                    } catch (InterruptedException e) {
                                        throw new RuntimeException(e);
                                    }
                                    System.out.println(format(
                                            "Thread: %s, STOP signal: %s, data: %s",
                                            Thread.currentThread().getName(),
                                            integerSignal.getType(),
                                            integerSignal.get()));
                                }
                        );

        Mono.zip(firstWork, secondWork)
                .subscribe(aVoid -> {
                    System.out.println(format("Thread: %s, ZIP", Thread.currentThread().getName()));
                });


     //   TimeUnit.SECONDS.sleep(10);
        System.out.println("End");
    }


}
