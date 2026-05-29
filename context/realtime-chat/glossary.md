# realtime-chat 용어 사전

| 용어 | 설명 |
|------|------|
| Session | 하나의 실시간 대화 단위. 생성(`POST /sessions`)·참여·종료(`end`)되며 참여자와 이벤트를 가진다 |
| Participant | 세션에 참여한 사용자. join/leave 이벤트로 입퇴장이 기록된다 |
| Presence | 참여자의 접속/참여 상태(online/offline 등). 실시간으로 변동 |
| Event | 세션에서 발생한 모든 사건의 단위(메시지 송신/수정/삭제, join/leave, presence 변경 등). 이벤트 소싱의 원천 |
| Message | 대화 메시지. 전송/수정/삭제 이벤트로 상태가 바뀜. Event의 한 종류로 모델링 가능 |
| Sequence (seq) | 세션 내 이벤트의 순서를 정하는 단조 증가 값. 순서 뒤바뀜 판정·정렬 기준 |
| Timeline | 특정 시점 `at` 기준으로 복원된 세션 상태(참여자/메시지/메시지 상태) |
| Projection | 이벤트를 적용해 만든 조회용 읽기 모델(예: 현재 참여자 목록, 메시지 목록) |
| Snapshot | 특정 seq/시점까지의 상태를 저장한 것. 복원 시 전체 리플레이 대신 스냅샷+이후 이벤트만 적용 |
| Replay | 이벤트를 순서대로 다시 적용하여 상태를 재구성하는 과정 |
| Idempotency Key | 이벤트 수집 중복을 막기 위해 클라이언트가 부여하는 고유 키 |
| at (복원 시점) | `GET /sessions/{id}/timeline?at=...` 의 기준 시각/시퀀스 |
| unread count | (선택) 참여자별 안 읽은 메시지 수. 복원 시 부가 상태 |
| typing indicator | (선택) 입력 중 표시 상태. 복원 시 부가 상태 |
| Transactional Outbox | 이벤트 DB 저장과 브로커 발행의 이중쓰기를 피하려 같은 트랜잭션에 event+outbox를 쓰고 커밋 후 발행하는 패턴 |
| seq-guard | projection worker가 `last_applied_seq`로 중복/순서 어긋난 재전달을 걸러 멱등 적용하는 가드 |
| Fan-out | 이벤트를 Redis Pub/Sub으로 전 인스턴스에 전파해 어느 노드에 붙은 참여자에게도 전달하는 것 |
| Projection lag | 이벤트 발생 시각과 읽기모델 반영 시각의 차이. 비동기 파이프라인 건강의 핵심 SLI |
| resume | 재연결 시 클라이언트의 마지막 `seq` 이후 이벤트만 재생해 정합성을 맞추는 것 |
