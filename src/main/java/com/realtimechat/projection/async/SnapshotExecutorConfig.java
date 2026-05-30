package com.realtimechat.projection.async;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SnapshotExecutorConfig {

    /**
     * 스냅샷 생성 전용 단일 스레드 executor. ProjectionApplier가 afterCommit에서 스냅샷 생성을
     * 이 executor로 위임하여 ProjectionWorker 소비 루프가 replay(restoreTo) 동안 블로킹되지 않게 한다.
     * 단일 스레드라 스냅샷이 직렬 처리되며(순서 안전), 스냅샷 PK 멱등(BR-5)으로 중복도 무해하다.
     * destroyMethod=shutdown: 컨텍스트 종료 시 진행 중 작업 완료 후 정리.
     */
    @Bean(destroyMethod = "shutdown")
    public ExecutorService snapshotExecutor() {
        return Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "snapshot-worker");
            t.setDaemon(true);
            return t;
        });
    }
}
