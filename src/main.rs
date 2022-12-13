use std::thread::sleep;
use std::time::Duration;

use service::server::tests::Harness;

#[tokio::main]
async fn main() {
    env_logger::init();
    //init_tracing("test").unwrap();
    test_election_basic().await;
    test_election_leader_disconnect().await;
    test_election_leader_and_another_disconnect().await;
    test_disconnect_all_then_restore().await;
    test_election_leader_disconnect_then_reconnect().await;
    test_leader_disconnect_then_reconnect_5().await;
    test_election_follower_comes_back().await;
    test_election_disconnect_loop().await;
}

async fn test_election_basic() {
    let h = Harness::new(3).await;
    let mut x = h.lock().await;
    x.check_single_leader().await;
    x.shutdown().await;
}

async fn test_election_leader_disconnect() {
    let h = Harness::new(3).await;
    let mut x = h.lock().await;
    let (orig_leader_id, orig_term_id) = x.check_single_leader().await;
    x.disconnect_peer(orig_leader_id as usize).await;
    sleep(Duration::from_millis(350));

    let (new_leader_id, new_term_id) = x.check_single_leader().await;
    if new_leader_id == orig_leader_id {
        eprintln!("new leader id to be different from original leader id");
    }

    if new_term_id < orig_term_id {
        eprintln!(
            "want new_term <= orig_term got {} and {}",
            new_term_id, orig_term_id
        );
    }
    x.shutdown().await;
}

async fn test_election_leader_and_another_disconnect() {
    let h = Harness::new(3).await;
    let mut x = h.lock().await;
    let (orig_leader_id, _) = x.check_single_leader().await;

    x.disconnect_peer(orig_leader_id as usize).await;

    let other_id = (orig_leader_id + 1) % 3;
    x.disconnect_peer(other_id as usize).await;

    // No Quorum
    sleep(Duration::from_millis(450));
    x.check_no_leader().await;

    x.reconnect_peer(other_id as usize).await.unwrap();
    x.check_single_leader().await;

    x.shutdown().await;
}

async fn test_disconnect_all_then_restore() {
    let h = Harness::new(3).await;
    let mut x = h.lock().await;

    sleep(Duration::from_millis(100));
    for i in 0..3 {
        x.disconnect_peer(i).await;
    }
    sleep(Duration::from_millis(450));
    x.check_no_leader().await;

    for i in 0..3 {
        x.reconnect_peer(i).await.unwrap();
    }
    x.check_single_leader().await;

    x.shutdown().await;
}

async fn test_election_leader_disconnect_then_reconnect() {
    let h = Harness::new(3).await;
    let mut x = h.lock().await;

    let (orig_leader_id, _) = x.check_single_leader().await;

    x.disconnect_peer(orig_leader_id as usize).await;

    sleep(Duration::from_millis(350));
    let (new_leader_id, new_term) = x.check_single_leader().await;

    x.reconnect_peer(orig_leader_id as usize).await.unwrap();
    sleep(Duration::from_millis(150));

    let (again_leader_id, again_term) = x.check_single_leader().await;

    if new_leader_id != again_leader_id {
        eprintln!(
            "again leader id got {}; want {}",
            again_leader_id, new_leader_id
        )
    }
    if again_term != new_term {
        eprintln!("again term got {}; want {}", again_term, new_term)
    }
    x.shutdown().await;
}

async fn test_leader_disconnect_then_reconnect_5() {
    let h = Harness::new(5).await;
    let mut x = h.lock().await;
    sleep(Duration::from_millis(2000));

    let (orig_leader_id, _) = x.check_single_leader().await;

    x.disconnect_peer(orig_leader_id as usize).await;
    sleep(Duration::from_millis(150));
    let (new_leader_id, new_term) = x.check_single_leader().await;

    x.reconnect_peer(orig_leader_id as usize).await.unwrap();
    sleep(Duration::from_millis(150));

    let (again_leader_id, again_term) = x.check_single_leader().await;

    if new_leader_id != again_leader_id {
        eprintln!(
            "again leader id got {}; want {}",
            again_leader_id, new_leader_id
        );
    }
    if again_term != new_term {
        eprintln!("again term got {}; want {}", again_term, new_term);
    }

    x.shutdown().await;
}

async fn test_election_follower_comes_back() {
    let h = Harness::new(3).await;
    let mut x = h.lock().await;

    let (orig_leader_id, orig_term) = x.check_single_leader().await;

    let other_id = (orig_leader_id + 1) % 3;
    x.disconnect_peer(other_id as usize).await;
    sleep(Duration::from_millis(650));
    x.reconnect_peer(other_id as usize).await.unwrap();
    sleep(Duration::from_millis(150));

    let (_, new_term) = x.check_single_leader().await;
    if new_term <= orig_term {
        eprintln!("newTerm={}, origTerm={}", new_term, orig_term);
    }
    x.shutdown().await;
}

async fn test_election_disconnect_loop() {
    let h = Harness::new(3).await;
    let mut x = h.lock().await;

    for _ in 0..5 {
        let (leader_id, _) = x.check_single_leader().await;

        x.disconnect_peer(leader_id as usize).await;
        let other_id = (leader_id + 1) % 3;
        x.disconnect_peer(other_id as usize).await;
        sleep(Duration::from_millis(310));
        x.check_no_leader().await;

        // Reconnect both.
        x.reconnect_peer(other_id as usize).await.unwrap();
        x.reconnect_peer(leader_id as usize).await.unwrap();

        // Give it time to settle
        sleep(Duration::from_millis(150));
    }
    x.shutdown().await;
}
