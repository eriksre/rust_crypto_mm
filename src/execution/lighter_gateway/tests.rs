use super::*;

use super::{
    CancelRef, ClientOrderId, LIGHTER_TX_TYPE_CANCEL_ORDER, LIGHTER_TX_TYPE_CREATE_ORDER,
    LighterGateway, LighterWsWorker, OrderState, OrderStatus, PendingSendTxBatch, Side, SignedTx,
    batch_observed_with_orders, collect_cancel_targets, order_status_is_terminal_for_cancel,
    parse_tx_lookup_response, reconcile_wait_for_batch,
};
use anyhow::anyhow;
use futures_util::SinkExt;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::io::duplex;
use tokio_tungstenite::{WebSocketStream, tungstenite::Message, tungstenite::protocol::Role};

#[test]
fn parse_sendtx_response_handles_data_attributes_shape() {
    let value = json!({
        "type": "jsonapi/response",
        "data": {
            "id": "txb-7",
            "attributes": {
                "code": "200",
                "message": "ok",
                "txHash": ["0xabc"]
            }
        }
    });
    let parsed = LighterWsWorker::parse_sendtx_response(&value).expect("parse sendtx response");
    assert_eq!(parsed.code, 200);
    assert_eq!(parsed.message.as_deref(), Some("ok"));
    assert_eq!(parsed.tx_hash, vec!["0xabc".to_string()]);
}

#[test]
fn parse_sendtx_error_handles_raw_error_frame() {
    let value = json!({
        "error": {
            "code": 23000,
            "message": "Too Many Requests!:  Not enough volume quota"
        },
        "id": "txb-9"
    });
    let err = LighterWsWorker::parse_sendtx_error(&value).expect("parse sendtx error");
    assert!(
        err.to_string()
            .contains("sendTxBatch error 23000: Too Many Requests!:  Not enough volume quota")
    );
}

#[test]
fn lighter_sendtx_quota_error_classifier_matches_volume_quota_rejection() {
    let err = "sendTxBatch error 23000: Too Many Requests!:  Not enough volume quota";
    assert!(super::is_lighter_sendtx_quota_error(err));
    assert!(!super::is_lighter_sendtx_quota_error(
        "sendTxBatch error 21700: invalid order index"
    ));
}

#[test]
fn validate_sendtx_response_hashes_rejects_hash_mismatch() {
    let mut batch = PendingSendTxBatch {
        tx_meta: Vec::new(),
        expected_hashes: vec!["0xabc".to_string()],
        observed_hashes: HashSet::new(),
        resp: None,
    };
    let response = super::SendTxBatchResponse {
        code: 200,
        message: Some("ok".to_string()),
        tx_hash: vec!["0xdef".to_string()],
    };
    let err = LighterWsWorker::validate_sendtx_response_hashes("txb-10", &mut batch, &response)
        .expect_err("mismatched hashes must fail loudly");
    assert!(err.to_string().contains("sendTxBatch hash mismatch"));
}

#[test]
fn value_has_sendtx_marker_on_code_and_hash_fields() {
    let value = json!({
        "id": 3,
        "code": 200,
        "tx_hash": ["0x1"]
    });
    assert!(LighterWsWorker::value_has_sendtx_marker(&value));
}

#[test]
fn extract_sendtx_req_id_reads_numeric_id() {
    let value = json!({"id": 42, "code": 200});
    assert_eq!(
        LighterWsWorker::extract_sendtx_req_id(&value).as_deref(),
        Some("42")
    );
}

#[test]
fn collect_account_tx_order_refs_parses_live_create_and_cancel_shapes() {
    let create = super::LighterTxEntry {
            hash: Some("0xcreate".to_string()),
            tx_type: Some(LIGHTER_TX_TYPE_CREATE_ORDER),
            status: Some(2),
            message: None,
            event_info: Some(r#"{"m":7,"t":{"p":0,"s":0,"tf":0,"mf":0},"mo":{"i":0,"u":0,"a":0,"is":0,"p":0,"rs":0,"ia":0,"ot":0,"f":0,"ro":0,"tp":0,"e":0,"st":0,"ts":0,"t0":0,"t1":0,"c0":0},"to":{"i":2251800161206198,"u":1,"a":498195,"is":20,"p":1363280,"rs":20,"ia":1,"ot":0,"f":2,"ro":0,"tp":0,"e":1775286032674,"st":2,"ts":0,"t0":0,"t1":0,"c0":0},"ae":""}"#.to_string()),
            executed_at: Some(1772866832794),
        };
    let parsed =
        LighterWsWorker::parse_account_tx_event_info(&create).expect("parse create event_info");
    let refs = LighterWsWorker::collect_account_tx_order_refs(
        LIGHTER_TX_TYPE_CREATE_ORDER,
        parsed.as_ref(),
    );
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0].client_order_index, 1);
    assert_eq!(refs[0].order_index, 2251800161206198);

    let cancel = super::LighterTxEntry {
        hash: Some("0xcancel".to_string()),
        tx_type: Some(LIGHTER_TX_TYPE_CANCEL_ORDER),
        status: Some(2),
        message: None,
        event_info: Some(r#"{"a":498195,"i":2251800161206198,"u":1,"ae":""}"#.to_string()),
        executed_at: Some(1772866833794),
    };
    let parsed =
        LighterWsWorker::parse_account_tx_event_info(&cancel).expect("parse cancel event_info");
    let refs = LighterWsWorker::collect_account_tx_order_refs(
        LIGHTER_TX_TYPE_CANCEL_ORDER,
        parsed.as_ref(),
    );
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0].client_order_index, 1);
    assert_eq!(refs[0].order_index, 2251800161206198);
}

#[tokio::test]
async fn pending_batch_completes_from_account_tx_hash() {
    let mut pending = HashMap::new();
    let mut pending_by_hash = HashMap::new();
    LighterWsWorker::insert_pending_batch(
        &mut pending,
        &mut pending_by_hash,
        "txb-1".to_string(),
        PendingSendTxBatch {
            tx_meta: Vec::new(),
            expected_hashes: vec!["0xabc".to_string()],
            observed_hashes: HashSet::new(),
            resp: None,
        },
    )
    .expect("insert pending batch");

    let tx = super::LighterTxEntry {
        hash: Some("0xabc".to_string()),
        tx_type: Some(LIGHTER_TX_TYPE_CREATE_ORDER),
        status: Some(2),
        message: None,
        event_info: Some(r#"{"ae":""}"#.to_string()),
        executed_at: Some(123),
    };

    let (req_id, result) =
        LighterWsWorker::update_pending_batch_from_account_tx(&tx, &mut pending, &pending_by_hash)
            .expect("matching account_tx should complete batch");
    assert_eq!(req_id, "txb-1");
    let payload = result.expect("success payload");
    assert_eq!(payload.code, 200);
    assert_eq!(payload.message.as_deref(), Some("confirmed via account_tx"));
    assert_eq!(payload.tx_hash, vec!["0xabc".to_string()]);
    let removed =
        LighterWsWorker::complete_pending_batch(&mut pending, &mut pending_by_hash, &req_id)
            .expect("remove pending batch");
    assert_eq!(removed.expected_hashes, vec!["0xabc".to_string()]);
    assert!(pending.is_empty());
    assert!(pending_by_hash.is_empty());
}

#[tokio::test]
async fn pending_batch_fails_from_account_tx_application_error() {
    let mut pending = HashMap::new();
    let mut pending_by_hash = HashMap::new();
    LighterWsWorker::insert_pending_batch(
        &mut pending,
        &mut pending_by_hash,
        "txb-2".to_string(),
        PendingSendTxBatch {
            tx_meta: Vec::new(),
            expected_hashes: vec!["0xdef".to_string()],
            observed_hashes: HashSet::new(),
            resp: None,
        },
    )
    .expect("insert pending batch");

    let tx = super::LighterTxEntry {
        hash: Some("0xdef".to_string()),
        tx_type: Some(LIGHTER_TX_TYPE_CANCEL_ORDER),
        status: Some(2),
        message: None,
        event_info: Some(
            r#"{"ae":"{\"code\":21700,\"message\":\"invalid order index\"}"}"#.to_string(),
        ),
        executed_at: Some(456),
    };

    let (req_id, result) =
        LighterWsWorker::update_pending_batch_from_account_tx(&tx, &mut pending, &pending_by_hash)
            .expect("matching account_tx should fail batch");
    assert_eq!(req_id, "txb-2");
    let err = result.expect_err("account_tx error should reject batch");
    assert!(err.to_string().contains("invalid order index"));
    let removed =
        LighterWsWorker::complete_pending_batch(&mut pending, &mut pending_by_hash, &req_id)
            .expect("remove pending batch");
    assert_eq!(removed.expected_hashes, vec!["0xdef".to_string()]);
    assert!(pending.is_empty());
    assert!(pending_by_hash.is_empty());
}

#[tokio::test]
async fn pending_batch_waits_for_all_expected_hashes() {
    let mut pending = HashMap::new();
    let mut pending_by_hash = HashMap::new();
    LighterWsWorker::insert_pending_batch(
        &mut pending,
        &mut pending_by_hash,
        "txb-3".to_string(),
        PendingSendTxBatch {
            tx_meta: Vec::new(),
            expected_hashes: vec!["0x111".to_string(), "0x222".to_string()],
            observed_hashes: HashSet::new(),
            resp: None,
        },
    )
    .expect("insert pending batch");

    let first = super::LighterTxEntry {
        hash: Some("0x111".to_string()),
        tx_type: Some(LIGHTER_TX_TYPE_CANCEL_ORDER),
        status: Some(2),
        message: None,
        event_info: Some(r#"{"ae":""}"#.to_string()),
        executed_at: Some(100),
    };
    assert!(
        LighterWsWorker::update_pending_batch_from_account_tx(
            &first,
            &mut pending,
            &pending_by_hash
        )
        .is_none()
    );
    assert_eq!(
        pending.get("txb-3").expect("pending batch").observed_hashes,
        HashSet::from(["0x111".to_string()])
    );

    let second = super::LighterTxEntry {
        hash: Some("0x222".to_string()),
        tx_type: Some(LIGHTER_TX_TYPE_CREATE_ORDER),
        status: Some(2),
        message: None,
        event_info: Some(r#"{"ae":""}"#.to_string()),
        executed_at: Some(101),
    };
    let (req_id, result) = LighterWsWorker::update_pending_batch_from_account_tx(
        &second,
        &mut pending,
        &pending_by_hash,
    )
    .expect("second hash should complete batch");
    assert_eq!(req_id, "txb-3");
    let payload = result.expect("success payload");
    assert_eq!(
        payload.tx_hash,
        vec!["0x111".to_string(), "0x222".to_string()]
    );
    let removed =
        LighterWsWorker::complete_pending_batch(&mut pending, &mut pending_by_hash, &req_id)
            .expect("remove pending batch");
    assert_eq!(
        removed.expected_hashes,
        vec!["0x111".to_string(), "0x222".to_string()]
    );
}

#[tokio::test]
async fn remove_pending_batch_matches_unprefixed_response_id() {
    let mut pending = HashMap::new();
    let mut pending_by_hash = HashMap::new();
    LighterWsWorker::insert_pending_batch(
        &mut pending,
        &mut pending_by_hash,
        "txb-7".to_string(),
        PendingSendTxBatch {
            tx_meta: Vec::new(),
            expected_hashes: vec!["0x777".to_string()],
            observed_hashes: HashSet::new(),
            resp: None,
        },
    )
    .expect("insert pending batch");

    let (req_id, batch) = LighterWsWorker::remove_pending_batch(
        &mut pending,
        &mut pending_by_hash,
        Some("7".to_string()),
        false,
    )
    .expect("response id should match prefixed pending request");
    assert_eq!(req_id, "txb-7");
    assert_eq!(batch.expected_hashes, vec!["0x777".to_string()]);
    assert!(pending.is_empty());
    assert!(pending_by_hash.is_empty());
}

#[test]
fn batch_observed_with_orders_recognizes_create_and_cancel_completion() {
    let create_id = ClientOrderId::new("mf-lighter-b-1");
    let cancel_id = ClientOrderId::new("mf-lighter-s-2");

    let mut orders = HashMap::new();
    orders.insert(
        create_id.clone(),
        OrderState {
            client_order_index: 1,
            order_index: Some(1001),
            side: Side::Bid,
            price: 100.0,
            size: 1.0,
            filled: 0.0,
            status: OrderStatus::New,
            exchange_order_id: None,
        },
    );
    orders.insert(
        cancel_id.clone(),
        OrderState {
            client_order_index: 2,
            order_index: Some(1002),
            side: Side::Ask,
            price: 101.0,
            size: 1.0,
            filled: 0.0,
            status: OrderStatus::Canceled,
            exchange_order_id: None,
        },
    );

    let tx_meta = vec![
        (LIGHTER_TX_TYPE_CREATE_ORDER, create_id),
        (LIGHTER_TX_TYPE_CANCEL_ORDER, cancel_id),
    ];
    assert!(batch_observed_with_orders(&orders, &tx_meta));
}

#[test]
fn batch_observed_with_orders_rejects_unconfirmed_create() {
    let create_id = ClientOrderId::new("mf-lighter-b-3");
    let mut orders = HashMap::new();
    orders.insert(
        create_id.clone(),
        OrderState {
            client_order_index: 3,
            order_index: None,
            side: Side::Bid,
            price: 100.0,
            size: 1.0,
            filled: 0.0,
            status: OrderStatus::New,
            exchange_order_id: None,
        },
    );
    let tx_meta = vec![(LIGHTER_TX_TYPE_CREATE_ORDER, create_id)];
    assert!(!batch_observed_with_orders(&orders, &tx_meta));
}

#[test]
fn batch_observed_with_orders_rejects_non_terminal_cancel() {
    let cancel_id = ClientOrderId::new("mf-lighter-c-3");
    let mut orders = HashMap::new();
    orders.insert(
        cancel_id.clone(),
        OrderState {
            client_order_index: 4,
            order_index: Some(2004),
            side: Side::Ask,
            price: 101.0,
            size: 1.0,
            filled: 0.0,
            status: OrderStatus::New,
            exchange_order_id: None,
        },
    );

    let tx_meta = vec![(LIGHTER_TX_TYPE_CANCEL_ORDER, cancel_id)];
    assert!(!batch_observed_with_orders(&orders, &tx_meta));
}

#[test]
fn collect_cancel_targets_prefers_confirmed_exchange_order_index() {
    let cancel_id = ClientOrderId::new("mf-lighter-s-9");
    let mut orders = HashMap::new();
    orders.insert(
        cancel_id.clone(),
        OrderState {
            client_order_index: 9,
            order_index: Some(1009),
            side: Side::Ask,
            price: 101.0,
            size: 1.0,
            filled: 0.0,
            status: OrderStatus::New,
            exchange_order_id: None,
        },
    );

    let targets = collect_cancel_targets(&orders, &[cancel_id], true)
        .expect("confirmed order_index should be used");
    assert_eq!(
        targets,
        vec![(
            ClientOrderId::new("mf-lighter-s-9"),
            CancelRef::ExchangeOrderIndex(1009)
        )]
    );
}

#[test]
fn collect_cancel_targets_falls_back_to_client_order_index_for_open_order() {
    let cancel_id = ClientOrderId::new("mf-lighter-s-10");
    let mut orders = HashMap::new();
    orders.insert(
        cancel_id.clone(),
        OrderState {
            client_order_index: 10,
            order_index: None,
            side: Side::Ask,
            price: 101.0,
            size: 1.0,
            filled: 0.0,
            status: OrderStatus::New,
            exchange_order_id: None,
        },
    );

    let targets = collect_cancel_targets(&orders, &[cancel_id], true)
        .expect("open order should fall back to client_order_index");
    assert_eq!(
        targets,
        vec![(
            ClientOrderId::new("mf-lighter-s-10"),
            CancelRef::ClientOrderIndex(10)
        )]
    );
}

#[test]
fn collect_cancel_targets_rejects_terminal_order_without_confirmed_index() {
    let cancel_id = ClientOrderId::new("mf-lighter-s-11");
    let mut orders = HashMap::new();
    orders.insert(
        cancel_id.clone(),
        OrderState {
            client_order_index: 11,
            order_index: None,
            side: Side::Ask,
            price: 101.0,
            size: 1.0,
            filled: 1.0,
            status: OrderStatus::Filled,
            exchange_order_id: None,
        },
    );

    let err = collect_cancel_targets(&orders, &[cancel_id], true)
        .expect_err("terminal order should fail loudly");
    assert!(
        err.to_string()
            .contains("refusing client_order_index fallback")
    );
}

#[test]
fn order_status_is_terminal_for_cancel_only_for_terminal_statuses() {
    assert!(!order_status_is_terminal_for_cancel(&OrderStatus::New));
    assert!(!order_status_is_terminal_for_cancel(
        &OrderStatus::PartiallyFilled
    ));
    assert!(order_status_is_terminal_for_cancel(&OrderStatus::Canceled));
    assert!(order_status_is_terminal_for_cancel(&OrderStatus::Filled));
    assert!(order_status_is_terminal_for_cancel(&OrderStatus::Rejected));
}

#[test]
fn is_nonce_consuming_failure_matches_confirmed_account_tx_failure() {
    let err = anyhow!(
        "lighter account_tx reported tx failure tx_type=15 hash=0xabc detail=invalid order index"
    );
    assert!(LighterGateway::is_nonce_consuming_failure(&err));
    let err = anyhow!("sendTxBatch error 21104: invalid nonce");
    assert!(!LighterGateway::is_nonce_consuming_failure(&err));
}

#[test]
fn update_from_entry_emits_new_when_order_index_is_first_confirmed() {
    let id = ClientOrderId::new("mf-lighter-b-10");
    let mut state = OrderState {
        client_order_index: 10,
        order_index: None,
        side: Side::Bid,
        price: 100.0,
        size: 1.0,
        filled: 0.0,
        status: OrderStatus::New,
        exchange_order_id: None,
    };
    let entry = super::LighterOrderEntry {
        order_index: Some(12345),
        client_order_index: Some(10),
        market_index: Some(1),
        price: Some("100".to_string()),
        initial_base_amount: Some("1000".to_string()),
        remaining_base_amount: Some("1000".to_string()),
        filled_base_amount: Some("0".to_string()),
        is_ask: Some(false),
        status: Some("open".to_string()),
        timestamp: Some(1),
    };
    let mut reports = Vec::new();

    super::update_from_entry(1000.0, &entry, &mut state, &id, &mut reports);

    assert_eq!(state.order_index, Some(12345));
    assert_eq!(reports.len(), 1);
    assert!(matches!(reports[0].status, OrderStatus::New));
}

#[test]
fn reconcile_wait_for_batch_extends_when_cancel_present() {
    let tx_meta_with_cancel = vec![(
        LIGHTER_TX_TYPE_CANCEL_ORDER,
        ClientOrderId::new("mf-lighter-c-1"),
    )];
    let tx_meta_create_only = vec![(
        LIGHTER_TX_TYPE_CREATE_ORDER,
        ClientOrderId::new("mf-lighter-n-1"),
    )];
    assert_eq!(
        reconcile_wait_for_batch(&tx_meta_with_cancel),
        Duration::from_secs(20)
    );
    assert_eq!(
        reconcile_wait_for_batch(&tx_meta_create_only),
        Duration::from_secs(8)
    );
}

#[test]
fn map_status_supports_common_terminal_aliases() {
    assert_eq!(super::map_status("cancelled"), OrderStatus::Canceled);
    assert_eq!(
        super::map_status("canceled-post-only"),
        OrderStatus::Canceled
    );
    assert_eq!(super::map_status("closed"), OrderStatus::Canceled);
    assert_eq!(super::map_status("rejected"), OrderStatus::Rejected);
    assert_eq!(
        super::map_status("partially_filled"),
        OrderStatus::PartiallyFilled
    );
}

#[test]
fn parse_tx_lookup_response_accepts_valid_payload() {
    let body = r#"{
            "code": 200,
            "hash": "0xabc",
            "type": 15,
            "status": 2,
            "nonce": 77,
            "queued_at": 101,
            "executed_at": 202,
            "event_info": "{\"ok\":true}"
        }"#;
    let parsed = parse_tx_lookup_response(body, "0xabc").expect("tx lookup should parse");
    assert_eq!(parsed.hash, "0xabc");
    assert_eq!(parsed.tx_type, 15);
    assert_eq!(parsed.status, 2);
    assert_eq!(parsed.nonce, 77);
}

#[test]
fn parse_tx_lookup_response_rejects_empty_hash() {
    let body = r#"{
            "code": 200,
            "hash": "",
            "type": 15,
            "status": 0,
            "nonce": 0,
            "queued_at": 0,
            "executed_at": 0,
            "event_info": ""
        }"#;
    let err = parse_tx_lookup_response(body, "0xdead").expect_err("empty hash must fail");
    assert!(err.to_string().contains("missing hash"));
}

#[test]
fn collect_expected_tx_hashes_rejects_duplicate_hashes() {
    let txs = vec![
        (
            SignedTx {
                tx_type: LIGHTER_TX_TYPE_CREATE_ORDER,
                tx_info: "a".to_string(),
                tx_hash: Some("0xdup".to_string()),
            },
            ClientOrderId::new("mf-lighter-a"),
        ),
        (
            SignedTx {
                tx_type: LIGHTER_TX_TYPE_CANCEL_ORDER,
                tx_info: "b".to_string(),
                tx_hash: Some("0xdup".to_string()),
            },
            ClientOrderId::new("mf-lighter-b"),
        ),
    ];

    let err = LighterWsWorker::collect_expected_tx_hashes(&txs)
        .expect_err("duplicate tx hashes must fail");
    assert!(err.to_string().contains("duplicate signer tx hash"));
}

#[tokio::test]
async fn await_initialization_message_accepts_first_server_frame() {
    let (client_io, server_io) = duplex(1024);
    let mut client_ws = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;

    let server = tokio::spawn(async move {
        let mut ws = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
        ws.send(Message::Text(r#"{"type":"welcome"}"#.into()))
            .await
            .expect("send init frame");
        ws.close(None).await.expect("close websocket");
    });

    LighterWsWorker::await_initialization_message(&mut client_ws, false)
        .await
        .expect("initialization frame should be accepted");

    server.await.expect("server task");
}
