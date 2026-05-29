package com.realtimechat.event;

/**
 * {@link EventStore#append} 결과.
 *
 * <p>append는 멱등 충돌(이미 같은 (session_id, idempotency_key)가 존재) 시 기존 이벤트를 그대로
 * 반환한다. 이 경우 projection·broadcast를 다시 수행하면 안 되므로(중복 적용/전달 방지),
 * 신규 INSERT 여부({@code isNew})를 함께 돌려준다.
 *
 * <ul>
 *   <li>신규 INSERT 성공 → {@code isNew=true}: CommandHandler가 projection·broadcast를 수행.</li>
 *   <li>선조회 반환 / race 재조회 → {@code isNew=false}: 멱등 재유입이므로 기존 이벤트만 반환하고
 *       projection·broadcast는 생략(이미 첫 처리 시 수행됨).</li>
 * </ul>
 *
 * @param event 저장(또는 멱등 충돌 시 기존)된 이벤트
 * @param isNew 이번 호출에서 새로 INSERT되었으면 true, 멱등 재유입이면 false
 */
public record AppendResult(StoredEvent event, boolean isNew) {
}
