use std::sync::Arc;
use std::sync::Barrier;
use std::thread;

use codex_protocol::ThreadId;
use pretty_assertions::assert_eq;

use super::CancellationReason;
use super::CompletionReceiptStore;
use super::ExitPublicationOutcome;
use super::InitialResponseDecision;
use super::InitialResponseOutcome;
use super::MAX_COMPLETION_RECEIPTS;
use super::ReceiptError;
use super::ReceiptId;
use super::ReceiptOwner;
use super::ReceiptStatus;
use super::SamplingSource;
use super::TerminalCompletion;

fn receipt_owner(runtime_generation: u64) -> ReceiptOwner {
    ReceiptOwner::new(
        ThreadId::from_u128(0x018f_0000_0000_7000_8000_0000_0000_0001),
        runtime_generation,
        "call-1",
    )
    .expect("test owner should be valid")
}

fn foreign_receipt_owners(runtime_generation: u64) -> [ReceiptOwner; 3] {
    [
        ReceiptOwner::new(
            ThreadId::from_u128(0x018f_0000_0000_7000_8000_0000_0000_0002),
            runtime_generation,
            "call-1",
        )
        .expect("foreign thread owner should be valid"),
        receipt_owner(runtime_generation + 1),
        ReceiptOwner::new(
            ThreadId::from_u128(0x018f_0000_0000_7000_8000_0000_0000_0001),
            runtime_generation,
            "call-2",
        )
        .expect("foreign call owner should be valid"),
    ]
}

fn completion(exit_code: Option<i32>) -> TerminalCompletion {
    TerminalCompletion {
        exit_code,
        timed_out: false,
    }
}

fn queued_receipt(store: &CompletionReceiptStore, owner: &ReceiptOwner) -> super::ReceiptId {
    let receipt_id = store
        .reserve(owner.clone())
        .expect("reservation should succeed");
    assert_eq!(
        store.resolve_initial_response(receipt_id, owner, InitialResponseDecision::Arm),
        Ok(InitialResponseOutcome::Armed)
    );
    assert_eq!(
        store.publish_exit(receipt_id, owner, completion(Some(0))),
        Ok(ExitPublicationOutcome::Queued)
    );
    receipt_id
}

#[test]
fn completion_receipt_happy_path_samples_exactly_once() {
    let store = CompletionReceiptStore::default();
    let owner = receipt_owner(1);
    let expected_completion = completion(Some(17));
    let receipt_id = store
        .reserve(owner.clone())
        .expect("reservation should succeed");

    assert_eq!(
        store.status(receipt_id, &owner),
        Ok(ReceiptStatus::Reserved)
    );
    assert_eq!(
        store.resolve_initial_response(receipt_id, &owner, InitialResponseDecision::Arm),
        Ok(InitialResponseOutcome::Armed)
    );
    assert_eq!(
        store.publish_exit(receipt_id, &owner, expected_completion),
        Ok(ExitPublicationOutcome::Queued)
    );

    let lease = store
        .lease_for_sampling(receipt_id, &owner, SamplingSource::PushedCompletion)
        .expect("queued receipt should lease");
    assert_eq!(
        store.status(receipt_id, &owner),
        Ok(ReceiptStatus::LeasedToSampling {
            source: SamplingSource::PushedCompletion,
        })
    );
    assert_eq!(store.acknowledge_sampled(&lease), Ok(expected_completion));
    assert_eq!(
        store.status(receipt_id, &owner),
        Ok(ReceiptStatus::Sampled {
            source: SamplingSource::PushedCompletion,
        })
    );
}

#[test]
fn completion_receipt_rejects_a_second_claim_after_sampling() {
    let store = CompletionReceiptStore::default();
    let owner = receipt_owner(2);
    let receipt_id = queued_receipt(&store, &owner);
    let lease = store
        .lease_for_sampling(receipt_id, &owner, SamplingSource::PushedCompletion)
        .expect("queued receipt should lease");
    store
        .acknowledge_sampled(&lease)
        .expect("first claim should sample");

    assert_eq!(
        store.lease_for_sampling(receipt_id, &owner, SamplingSource::PushedCompletion),
        Err(ReceiptError::AlreadyConsumed)
    );
}

#[test]
fn completion_receipt_exit_and_initial_response_are_linearized_in_both_orders() {
    let store = Arc::new(CompletionReceiptStore::default());
    let owner = receipt_owner(3);
    let receipt_id = store
        .reserve(owner.clone())
        .expect("reservation should succeed");
    let exit_published = Arc::new(Barrier::new(2));

    let publisher_store = Arc::clone(&store);
    let publisher_owner = owner.clone();
    let publisher_barrier = Arc::clone(&exit_published);
    let publisher = thread::spawn(move || {
        let result =
            publisher_store.publish_exit(receipt_id, &publisher_owner, completion(Some(23)));
        publisher_barrier.wait();
        result
    });

    exit_published.wait();
    assert_eq!(
        publisher.join().expect("publisher should not panic"),
        Ok(ExitPublicationOutcome::RetainedUntilDecision)
    );
    assert_eq!(
        store.resolve_initial_response(receipt_id, &owner, InitialResponseDecision::InlineResult,),
        Ok(InitialResponseOutcome::InlineResult(completion(Some(23))))
    );
    assert_eq!(
        store.status(receipt_id, &owner),
        Ok(ReceiptStatus::InlineResult)
    );
    assert_eq!(
        store.lease_for_sampling(receipt_id, &owner, SamplingSource::PushedCompletion),
        Err(ReceiptError::InvalidTransition {
            actual: ReceiptStatus::InlineResult,
        })
    );

    let early_armed_store = Arc::new(CompletionReceiptStore::default());
    let early_armed_owner = receipt_owner(30);
    let early_armed_receipt_id = early_armed_store
        .reserve(early_armed_owner.clone())
        .expect("reservation should succeed");
    let exit_published_before_arm = Arc::new(Barrier::new(2));
    let publisher_store = Arc::clone(&early_armed_store);
    let publisher_owner = early_armed_owner.clone();
    let publisher_barrier = Arc::clone(&exit_published_before_arm);
    let publisher = thread::spawn(move || {
        let result = publisher_store.publish_exit(
            early_armed_receipt_id,
            &publisher_owner,
            completion(Some(27)),
        );
        publisher_barrier.wait();
        result
    });

    exit_published_before_arm.wait();
    assert_eq!(
        publisher.join().expect("publisher should not panic"),
        Ok(ExitPublicationOutcome::RetainedUntilDecision)
    );
    assert_eq!(
        early_armed_store.resolve_initial_response(
            early_armed_receipt_id,
            &early_armed_owner,
            InitialResponseDecision::Arm,
        ),
        Ok(InitialResponseOutcome::Queued(completion(Some(27))))
    );
    let lease = early_armed_store
        .lease_for_sampling(
            early_armed_receipt_id,
            &early_armed_owner,
            SamplingSource::PushedCompletion,
        )
        .expect("exit retained before arming should be claimable");
    assert_eq!(
        early_armed_store.acknowledge_sampled(&lease),
        Ok(completion(Some(27)))
    );

    let armed_store = Arc::new(CompletionReceiptStore::default());
    let armed_owner = receipt_owner(4);
    let armed_receipt_id = armed_store
        .reserve(armed_owner.clone())
        .expect("reservation should succeed");
    let armed_before_publish = Arc::new(Barrier::new(2));
    let response_store = Arc::clone(&armed_store);
    let response_owner = armed_owner.clone();
    let response_barrier = Arc::clone(&armed_before_publish);
    let response = thread::spawn(move || {
        let result = response_store.resolve_initial_response(
            armed_receipt_id,
            &response_owner,
            InitialResponseDecision::Arm,
        );
        response_barrier.wait();
        result
    });

    let publisher_store = Arc::clone(&armed_store);
    let publisher_owner = armed_owner.clone();
    let publisher_barrier = Arc::clone(&armed_before_publish);
    let publisher = thread::spawn(move || {
        publisher_barrier.wait();
        publisher_store.publish_exit(armed_receipt_id, &publisher_owner, completion(Some(29)))
    });

    assert_eq!(
        response.join().expect("response decision should not panic"),
        Ok(InitialResponseOutcome::Armed)
    );
    assert_eq!(
        publisher.join().expect("publisher should not panic"),
        Ok(ExitPublicationOutcome::Queued)
    );
    assert_eq!(
        armed_store.status(armed_receipt_id, &armed_owner),
        Ok(ReceiptStatus::Queued)
    );
    let lease = armed_store
        .lease_for_sampling(
            armed_receipt_id,
            &armed_owner,
            SamplingSource::PushedCompletion,
        )
        .expect("retained exit should be claimable once armed");
    assert_eq!(
        armed_store.acknowledge_sampled(&lease),
        Ok(completion(Some(29)))
    );
    assert_eq!(
        armed_store.lease_for_sampling(
            armed_receipt_id,
            &armed_owner,
            SamplingSource::PushedCompletion,
        ),
        Err(ReceiptError::AlreadyConsumed)
    );
}

#[test]
fn completion_receipt_inline_decision_waits_for_finalized_exit() {
    let store = CompletionReceiptStore::default();
    let owner = receipt_owner(5);
    let receipt_id = store
        .reserve(owner.clone())
        .expect("reservation should succeed");

    assert_eq!(
        store.resolve_initial_response(receipt_id, &owner, InitialResponseDecision::InlineResult,),
        Err(ReceiptError::InvalidTransition {
            actual: ReceiptStatus::Reserved,
        })
    );
    assert_eq!(
        store.status(receipt_id, &owner),
        Ok(ReceiptStatus::Reserved)
    );
    assert_eq!(
        store.publish_exit(receipt_id, &owner, completion(Some(31))),
        Ok(ExitPublicationOutcome::RetainedUntilDecision)
    );
    assert_eq!(
        store.resolve_initial_response(receipt_id, &owner, InitialResponseDecision::InlineResult,),
        Ok(InitialResponseOutcome::InlineResult(completion(Some(31))))
    );
}

#[test]
fn completion_receipt_failed_lease_requeues_same_receipt_and_rejects_stale_token() {
    let store = CompletionReceiptStore::default();
    let owner = receipt_owner(6);
    let receipt_id = queued_receipt(&store, &owner);
    let first_lease = store
        .lease_for_sampling(receipt_id, &owner, SamplingSource::PushedCompletion)
        .expect("first lease should succeed");

    store
        .fail_sampling(&first_lease)
        .expect("failed lease should return to the queue");
    assert_eq!(store.status(receipt_id, &owner), Ok(ReceiptStatus::Queued));
    let second_lease = store
        .lease_for_sampling(receipt_id, &owner, SamplingSource::PushedCompletion)
        .expect("same receipt should be re-leased");
    assert_eq!(
        store.acknowledge_sampled(&first_lease),
        Err(ReceiptError::StaleLease)
    );
    assert_eq!(
        store.status(receipt_id, &owner),
        Ok(ReceiptStatus::LeasedToSampling {
            source: SamplingSource::PushedCompletion,
        })
    );
    assert_eq!(
        store.acknowledge_sampled(&second_lease),
        Ok(completion(Some(0)))
    );
}

#[test]
fn completion_receipt_stale_failed_lease_cannot_requeue_a_new_lease() {
    let store = CompletionReceiptStore::default();
    let owner = receipt_owner(61);
    let receipt_id = queued_receipt(&store, &owner);
    let first_lease = store
        .lease_for_sampling(receipt_id, &owner, SamplingSource::PushedCompletion)
        .expect("first lease should succeed");

    store
        .fail_sampling(&first_lease)
        .expect("first lease failure should requeue the receipt");
    let second_lease = store
        .lease_for_sampling(receipt_id, &owner, SamplingSource::PushedCompletion)
        .expect("same receipt should be re-leased");

    assert_eq!(
        store.fail_sampling(&first_lease),
        Err(ReceiptError::StaleLease)
    );
    assert_eq!(
        store.status(receipt_id, &owner),
        Ok(ReceiptStatus::LeasedToSampling {
            source: SamplingSource::PushedCompletion,
        })
    );
    assert_eq!(
        store.acknowledge_sampled(&second_lease),
        Ok(completion(Some(0)))
    );
}

#[test]
fn completion_receipt_cancellation_is_terminal_from_each_unsampled_state() {
    #[derive(Clone, Copy)]
    enum StartingState {
        Reserved,
        Armed,
        Queued,
        Leased,
    }

    for (index, (starting_state, reason)) in [
        (StartingState::Reserved, CancellationReason::Released),
        (StartingState::Armed, CancellationReason::OwnerStopped),
        (StartingState::Queued, CancellationReason::Shutdown),
        (StartingState::Leased, CancellationReason::Interrupted),
    ]
    .into_iter()
    .enumerate()
    {
        let store = CompletionReceiptStore::default();
        let owner = receipt_owner(index as u64 + 7);
        let receipt_id = store
            .reserve(owner.clone())
            .expect("reservation should succeed");
        let lease = match starting_state {
            StartingState::Reserved => None,
            StartingState::Armed => {
                assert_eq!(
                    store.resolve_initial_response(
                        receipt_id,
                        &owner,
                        InitialResponseDecision::Arm,
                    ),
                    Ok(InitialResponseOutcome::Armed)
                );
                None
            }
            StartingState::Queued | StartingState::Leased => {
                assert_eq!(
                    store.resolve_initial_response(
                        receipt_id,
                        &owner,
                        InitialResponseDecision::Arm,
                    ),
                    Ok(InitialResponseOutcome::Armed)
                );
                assert_eq!(
                    store.publish_exit(receipt_id, &owner, completion(Some(0))),
                    Ok(ExitPublicationOutcome::Queued)
                );
                if matches!(starting_state, StartingState::Leased) {
                    Some(
                        store
                            .lease_for_sampling(
                                receipt_id,
                                &owner,
                                SamplingSource::PushedCompletion,
                            )
                            .expect("queued receipt should lease"),
                    )
                } else {
                    None
                }
            }
        };

        store
            .cancel(receipt_id, &owner, reason)
            .expect("unsampled receipt should cancel");
        assert_eq!(
            store.status(receipt_id, &owner),
            Ok(ReceiptStatus::Cancelled { reason })
        );
        assert_eq!(
            store.lease_for_sampling(receipt_id, &owner, SamplingSource::PushedCompletion),
            Err(ReceiptError::Cancelled { reason })
        );
        if let Some(lease) = lease {
            assert_eq!(
                store.acknowledge_sampled(&lease),
                Err(ReceiptError::Cancelled { reason })
            );
        }
        assert!(
            store.reserve(owner).is_ok(),
            "cancellation must free a slot"
        );
    }
}

#[test]
fn completion_receipt_capacity_refuses_the_65th_and_reuses_a_freed_slot() {
    let store = CompletionReceiptStore::default();
    let owner = receipt_owner(11);
    let receipts = (0..MAX_COMPLETION_RECEIPTS)
        .map(|_| {
            store
                .reserve(owner.clone())
                .expect("capacity slot should reserve")
        })
        .collect::<Vec<_>>();

    assert_eq!(
        store.reserve(owner.clone()),
        Err(ReceiptError::CapacityExceeded {
            capacity: MAX_COMPLETION_RECEIPTS,
        })
    );
    store
        .cancel(receipts[0], &owner, CancellationReason::Released)
        .expect("released receipt should free capacity");
    assert!(
        store.reserve(owner).is_ok(),
        "freed capacity should be reusable"
    );
}

#[test]
fn completion_receipt_terminal_history_evicts_only_the_oldest_after_64_outcomes() {
    let store = CompletionReceiptStore::default();
    let owner = receipt_owner(66);
    let mut receipts = Vec::new();

    for _ in 0..=MAX_COMPLETION_RECEIPTS {
        let receipt_id = store
            .reserve(owner.clone())
            .expect("each retirement must free its capacity slot");
        assert_eq!(
            store.cancel(receipt_id, &owner, CancellationReason::Released),
            Ok(())
        );
        receipts.push(receipt_id);
    }

    assert_eq!(
        store.status(receipts[0], &owner),
        Err(ReceiptError::UnknownReceipt)
    );
    for receipt_id in &receipts[1..] {
        assert_eq!(
            store.status(*receipt_id, &owner),
            Ok(ReceiptStatus::Cancelled {
                reason: CancellationReason::Released,
            })
        );
    }
}

#[test]
fn completion_receipt_terminal_history_refuses_foreign_owners_and_repeat_cancellation() {
    let store = CompletionReceiptStore::default();
    let owner = receipt_owner(67);
    let sampled_receipt = queued_receipt(&store, &owner);
    let sampled_lease = store
        .lease_for_sampling(sampled_receipt, &owner, SamplingSource::PushedCompletion)
        .expect("queued receipt should lease");
    assert_eq!(
        store.acknowledge_sampled(&sampled_lease),
        Ok(completion(Some(0)))
    );

    let inline_receipt = store
        .reserve(owner.clone())
        .expect("inline receipt should reserve");
    assert_eq!(
        store.publish_exit(inline_receipt, &owner, completion(Some(17))),
        Ok(ExitPublicationOutcome::RetainedUntilDecision)
    );
    assert_eq!(
        store.resolve_initial_response(
            inline_receipt,
            &owner,
            InitialResponseDecision::InlineResult
        ),
        Ok(InitialResponseOutcome::InlineResult(completion(Some(17))))
    );

    let cancelled_receipt = queued_receipt(&store, &owner);
    assert_eq!(
        store.cancel(cancelled_receipt, &owner, CancellationReason::Shutdown),
        Ok(())
    );

    for (receipt_id, expected_status) in [
        (
            sampled_receipt,
            ReceiptStatus::Sampled {
                source: SamplingSource::PushedCompletion,
            },
        ),
        (inline_receipt, ReceiptStatus::InlineResult),
        (
            cancelled_receipt,
            ReceiptStatus::Cancelled {
                reason: CancellationReason::Shutdown,
            },
        ),
    ] {
        for foreign_owner in &foreign_receipt_owners(67) {
            assert_eq!(
                store.status(receipt_id, foreign_owner),
                Err(ReceiptError::ForeignOwner)
            );
            assert_eq!(
                store.cancel(receipt_id, foreign_owner, CancellationReason::Released),
                Err(ReceiptError::ForeignOwner)
            );
            assert_eq!(
                store.lease_for_sampling(
                    receipt_id,
                    foreign_owner,
                    SamplingSource::PushedCompletion
                ),
                Err(ReceiptError::ForeignOwner)
            );
            assert_eq!(
                store.resolve_initial_response(
                    receipt_id,
                    foreign_owner,
                    InitialResponseDecision::Arm
                ),
                Err(ReceiptError::ForeignOwner)
            );
            assert_eq!(
                store.publish_exit(receipt_id, foreign_owner, completion(Some(99))),
                Err(ReceiptError::ForeignOwner)
            );

            // Forge both target and owner from a real lease to attack the terminal lookup.
            let mut foreign_lease = sampled_lease.clone();
            foreign_lease.receipt_id = receipt_id;
            foreign_lease.owner = foreign_owner.clone();
            assert_eq!(
                store.fail_sampling(&foreign_lease),
                Err(ReceiptError::ForeignOwner)
            );
            assert_eq!(
                store.acknowledge_sampled(&foreign_lease),
                Err(ReceiptError::ForeignOwner)
            );
            assert_eq!(
                store.status(receipt_id, &owner),
                Ok(expected_status.clone())
            );
        }
        assert_eq!(
            store.cancel(receipt_id, &owner, CancellationReason::Released),
            Err(ReceiptError::AlreadyTerminal)
        );
        assert_eq!(store.status(receipt_id, &owner), Ok(expected_status));
    }
}

#[test]
fn completion_receipt_foreign_runtime_generation_is_refused() {
    let store = CompletionReceiptStore::default();
    let owner = receipt_owner(12);
    let foreign_owners = [
        receipt_owner(13),
        ReceiptOwner::new(
            ThreadId::from_u128(0x018f_0000_0000_7000_8000_0000_0000_0002),
            12,
            "call-1",
        )
        .expect("foreign thread owner should be valid"),
        ReceiptOwner::new(
            ThreadId::from_u128(0x018f_0000_0000_7000_8000_0000_0000_0001),
            12,
            "call-2",
        )
        .expect("foreign call owner should be valid"),
    ];
    let receipt_id = store
        .reserve(owner.clone())
        .expect("reservation should succeed");

    for foreign_owner in &foreign_owners {
        assert_eq!(
            store.status(receipt_id, foreign_owner),
            Err(ReceiptError::ForeignOwner)
        );
        assert_eq!(
            store.publish_exit(receipt_id, foreign_owner, completion(Some(0))),
            Err(ReceiptError::ForeignOwner)
        );
        assert_eq!(
            store.cancel(receipt_id, foreign_owner, CancellationReason::Released),
            Err(ReceiptError::ForeignOwner)
        );
    }
    assert_eq!(
        store.status(receipt_id, &owner),
        Ok(ReceiptStatus::Reserved)
    );
}

#[test]
fn completion_receipt_foreign_owner_is_refused_on_resolve_and_lease() {
    let store = CompletionReceiptStore::default();
    let owner = receipt_owner(62);
    let receipt_id = store
        .reserve(owner.clone())
        .expect("reservation should succeed");

    for foreign_owner in &foreign_receipt_owners(62) {
        assert_eq!(
            store
                .resolve_initial_response(receipt_id, foreign_owner, InitialResponseDecision::Arm,),
            Err(ReceiptError::ForeignOwner)
        );
    }
    assert_eq!(
        store.status(receipt_id, &owner),
        Ok(ReceiptStatus::Reserved)
    );
    assert_eq!(
        store.resolve_initial_response(receipt_id, &owner, InitialResponseDecision::Arm),
        Ok(InitialResponseOutcome::Armed)
    );
    assert_eq!(
        store.publish_exit(receipt_id, &owner, completion(Some(0))),
        Ok(ExitPublicationOutcome::Queued)
    );

    for foreign_owner in &foreign_receipt_owners(62) {
        assert_eq!(
            store.lease_for_sampling(receipt_id, foreign_owner, SamplingSource::PushedCompletion,),
            Err(ReceiptError::ForeignOwner)
        );
    }
    assert_eq!(store.status(receipt_id, &owner), Ok(ReceiptStatus::Queued));
}

#[test]
fn completion_receipt_foreign_lease_is_refused_on_fail_and_acknowledge() {
    let store = CompletionReceiptStore::default();
    let owner = receipt_owner(63);
    let receipt_id = queued_receipt(&store, &owner);
    let lease = store
        .lease_for_sampling(receipt_id, &owner, SamplingSource::PushedCompletion)
        .expect("owning receipt should lease");

    for foreign_owner in &foreign_receipt_owners(63) {
        let mut foreign_lease = lease.clone();
        foreign_lease.owner = foreign_owner.clone();
        assert_eq!(
            store.fail_sampling(&foreign_lease),
            Err(ReceiptError::ForeignOwner)
        );

        let mut foreign_lease = lease.clone();
        foreign_lease.owner = foreign_owner.clone();
        assert_eq!(
            store.acknowledge_sampled(&foreign_lease),
            Err(ReceiptError::ForeignOwner)
        );
    }

    assert_eq!(
        store.status(receipt_id, &owner),
        Ok(ReceiptStatus::LeasedToSampling {
            source: SamplingSource::PushedCompletion,
        })
    );
    assert_eq!(store.acknowledge_sampled(&lease), Ok(completion(Some(0))));
}

#[test]
fn completion_receipt_terminal_stdin_and_pushed_sources_share_one_lease() {
    let store = CompletionReceiptStore::default();
    let owner = receipt_owner(16);
    let receipt_id = queued_receipt(&store, &owner);
    let pushed_lease = store
        .lease_for_sampling(receipt_id, &owner, SamplingSource::PushedCompletion)
        .expect("pushed source should acquire the queued claim");

    assert_eq!(
        store.lease_for_sampling(receipt_id, &owner, SamplingSource::TerminalStdinOutput,),
        Err(ReceiptError::AlreadyLeased)
    );
    store
        .fail_sampling(&pushed_lease)
        .expect("failed pushed claim should requeue the same receipt");
    let stdin_lease = store
        .lease_for_sampling(receipt_id, &owner, SamplingSource::TerminalStdinOutput)
        .expect("terminal stdin source should acquire the requeued claim");
    assert_eq!(
        store.status(receipt_id, &owner),
        Ok(ReceiptStatus::LeasedToSampling {
            source: SamplingSource::TerminalStdinOutput,
        })
    );
    assert_eq!(
        store.acknowledge_sampled(&stdin_lease),
        Ok(completion(Some(0)))
    );
    assert_eq!(
        store.status(receipt_id, &owner),
        Ok(ReceiptStatus::Sampled {
            source: SamplingSource::TerminalStdinOutput,
        })
    );
    assert_eq!(
        store.lease_for_sampling(receipt_id, &owner, SamplingSource::PushedCompletion),
        Err(ReceiptError::AlreadyConsumed)
    );
}

#[test]
fn completion_receipt_terminal_stdin_and_pushed_claim_race_consumes_once() {
    let store = Arc::new(CompletionReceiptStore::default());
    let owner = receipt_owner(14);
    let receipt_id = queued_receipt(&store, &owner);
    let start = Arc::new(Barrier::new(3));
    let both_leases_attempted = Arc::new(Barrier::new(3));

    let pushed_store = Arc::clone(&store);
    let pushed_owner = owner.clone();
    let pushed_start = Arc::clone(&start);
    let pushed_claimed = Arc::clone(&both_leases_attempted);
    let pushed = thread::spawn(move || {
        pushed_start.wait();
        let lease = pushed_store.lease_for_sampling(
            receipt_id,
            &pushed_owner,
            SamplingSource::PushedCompletion,
        );
        pushed_claimed.wait();
        let result = match lease {
            Ok(lease) => pushed_store.acknowledge_sampled(&lease),
            Err(error) => Err(error),
        };
        (SamplingSource::PushedCompletion, result)
    });

    let stdin_store = Arc::clone(&store);
    let stdin_owner = owner.clone();
    let stdin_start = Arc::clone(&start);
    let stdin_claimed = Arc::clone(&both_leases_attempted);
    let stdin = thread::spawn(move || {
        stdin_start.wait();
        let lease = stdin_store.lease_for_sampling(
            receipt_id,
            &stdin_owner,
            SamplingSource::TerminalStdinOutput,
        );
        stdin_claimed.wait();
        let result = match lease {
            Ok(lease) => stdin_store.acknowledge_sampled(&lease),
            Err(error) => Err(error),
        };
        (SamplingSource::TerminalStdinOutput, result)
    });

    start.wait();
    both_leases_attempted.wait();
    let pushed = pushed.join().expect("pushed claim should not panic");
    let stdin = stdin.join().expect("stdin claim should not panic");
    let (winner, winner_result, loser, loser_result) = match (pushed, stdin) {
        (
            (SamplingSource::PushedCompletion, Ok(result)),
            (SamplingSource::TerminalStdinOutput, Err(error)),
        ) => (
            SamplingSource::PushedCompletion,
            result,
            SamplingSource::TerminalStdinOutput,
            error,
        ),
        (
            (SamplingSource::PushedCompletion, Err(error)),
            (SamplingSource::TerminalStdinOutput, Ok(result)),
        ) => (
            SamplingSource::TerminalStdinOutput,
            result,
            SamplingSource::PushedCompletion,
            error,
        ),
        _ => panic!("exactly one sampling path should win"),
    };
    assert_eq!(winner_result, completion(Some(0)));
    assert!(matches!(
        loser_result,
        ReceiptError::AlreadyLeased | ReceiptError::AlreadyConsumed
    ));
    assert_eq!(
        store.lease_for_sampling(receipt_id, &owner, loser),
        Err(ReceiptError::AlreadyConsumed)
    );
    assert_eq!(
        store.status(receipt_id, &owner),
        Ok(ReceiptStatus::Sampled { source: winner })
    );
}

#[test]
fn completion_receipt_refuses_duplicate_exit_publication() {
    let store = CompletionReceiptStore::default();
    let owner = receipt_owner(15);
    let receipt_id = queued_receipt(&store, &owner);

    assert_eq!(
        store.publish_exit(receipt_id, &owner, completion(Some(1))),
        Err(ReceiptError::InvalidTransition {
            actual: ReceiptStatus::Queued,
        })
    );
    assert_eq!(store.status(receipt_id, &owner), Ok(ReceiptStatus::Queued));
}

#[test]
fn completion_receipt_duplicate_exit_while_reserved_preserves_first_completion() {
    let inline_store = CompletionReceiptStore::default();
    let inline_owner = receipt_owner(64);
    let inline_receipt = inline_store
        .reserve(inline_owner.clone())
        .expect("reservation should succeed");
    let first_inline_completion = completion(Some(41));

    assert_eq!(
        inline_store.publish_exit(inline_receipt, &inline_owner, first_inline_completion),
        Ok(ExitPublicationOutcome::RetainedUntilDecision)
    );
    assert_eq!(
        inline_store.publish_exit(inline_receipt, &inline_owner, completion(Some(42))),
        Err(ReceiptError::InvalidTransition {
            actual: ReceiptStatus::Reserved,
        })
    );
    assert_eq!(
        inline_store.resolve_initial_response(
            inline_receipt,
            &inline_owner,
            InitialResponseDecision::InlineResult,
        ),
        Ok(InitialResponseOutcome::InlineResult(
            first_inline_completion
        ))
    );

    let queued_store = CompletionReceiptStore::default();
    let queued_owner = receipt_owner(65);
    let queued_receipt = queued_store
        .reserve(queued_owner.clone())
        .expect("reservation should succeed");
    let first_queued_completion = completion(Some(51));

    assert_eq!(
        queued_store.publish_exit(queued_receipt, &queued_owner, first_queued_completion),
        Ok(ExitPublicationOutcome::RetainedUntilDecision)
    );
    assert_eq!(
        queued_store.publish_exit(queued_receipt, &queued_owner, completion(Some(52))),
        Err(ReceiptError::InvalidTransition {
            actual: ReceiptStatus::Reserved,
        })
    );
    assert_eq!(
        queued_store.resolve_initial_response(
            queued_receipt,
            &queued_owner,
            InitialResponseDecision::Arm,
        ),
        Ok(InitialResponseOutcome::Queued(first_queued_completion))
    );
    let lease = queued_store
        .lease_for_sampling(
            queued_receipt,
            &queued_owner,
            SamplingSource::PushedCompletion,
        )
        .expect("first retained completion should lease");
    assert_eq!(
        queued_store.acknowledge_sampled(&lease),
        Ok(first_queued_completion)
    );
}

#[test]
fn completion_receipt_owner_requires_a_bounded_call_id() {
    assert_eq!(
        ReceiptOwner::new(
            ThreadId::from_u128(0x018f_0000_0000_7000_8000_0000_0001),
            1,
            "",
        ),
        Err(ReceiptError::InvalidOwner)
    );
    assert_eq!(
        ReceiptOwner::new(
            ThreadId::from_u128(0x018f_0000_0000_7000_8000_0000_0001),
            1,
            "x".repeat(257),
        ),
        Err(ReceiptError::InvalidOwner)
    );
}

#[test]
fn completion_receipt_owner_accepts_256_bytes_and_refuses_257() {
    let thread_id = ThreadId::from_u128(0x018f_0000_0000_7000_8000_0000_0000_0001);
    let owner = ReceiptOwner::new(thread_id, /*runtime_generation*/ 68, "x".repeat(256))
        .expect("a call id at the 256-byte limit must be accepted");
    let store = CompletionReceiptStore::default();
    let receipt_id = store
        .reserve(owner.clone())
        .expect("owner at the call id limit should reserve");
    assert_eq!(
        store.status(receipt_id, &owner),
        Ok(ReceiptStatus::Reserved)
    );
    assert_eq!(
        ReceiptOwner::new(thread_id, /*runtime_generation*/ 68, "x".repeat(257)),
        Err(ReceiptError::InvalidOwner)
    );
}

#[test]
fn completion_receipt_late_initial_response_refuses_each_active_nonreserved_state() {
    for expected_status in [
        ReceiptStatus::Armed,
        ReceiptStatus::Queued,
        ReceiptStatus::LeasedToSampling {
            source: SamplingSource::PushedCompletion,
        },
    ] {
        let store = CompletionReceiptStore::default();
        let owner = receipt_owner(/*runtime_generation*/ 69);
        let receipt_id = store
            .reserve(owner.clone())
            .expect("receipt should reserve");
        assert_eq!(
            store.resolve_initial_response(receipt_id, &owner, InitialResponseDecision::Arm),
            Ok(InitialResponseOutcome::Armed)
        );
        if expected_status != ReceiptStatus::Armed {
            assert_eq!(
                store.publish_exit(receipt_id, &owner, completion(Some(73))),
                Ok(ExitPublicationOutcome::Queued)
            );
        }
        let lease = if matches!(expected_status, ReceiptStatus::LeasedToSampling { .. }) {
            Some(
                store
                    .lease_for_sampling(receipt_id, &owner, SamplingSource::PushedCompletion)
                    .expect("queued receipt should lease"),
            )
        } else {
            None
        };
        for decision in [
            InitialResponseDecision::Arm,
            InitialResponseDecision::InlineResult,
        ] {
            assert_eq!(
                store.resolve_initial_response(receipt_id, &owner, decision),
                Err(ReceiptError::InvalidTransition {
                    actual: expected_status.clone(),
                })
            );
            assert_eq!(
                store.status(receipt_id, &owner),
                Ok(expected_status.clone())
            );
        }
        if expected_status == ReceiptStatus::Armed {
            assert_eq!(
                store.publish_exit(receipt_id, &owner, completion(Some(73))),
                Ok(ExitPublicationOutcome::Queued)
            );
        }
        let lease = lease.unwrap_or_else(|| {
            store
                .lease_for_sampling(receipt_id, &owner, SamplingSource::PushedCompletion)
                .expect("late decision must preserve the queued completion")
        });
        assert_eq!(store.acknowledge_sampled(&lease), Ok(completion(Some(73))));
    }
}

#[test]
fn completion_receipt_unqueued_and_mismatched_leases_preserve_active_state() {
    let store = CompletionReceiptStore::default();
    let owner = receipt_owner(/*runtime_generation*/ 70);
    let receipt_id = store
        .reserve(owner.clone())
        .expect("receipt should reserve");
    let donor_id = queued_receipt(&store, &owner);
    let donor = store
        .lease_for_sampling(donor_id, &owner, SamplingSource::PushedCompletion)
        .expect("donor receipt should lease");
    let mut wrong_phase = donor.clone();
    wrong_phase.receipt_id = receipt_id;
    for expected_status in [ReceiptStatus::Reserved, ReceiptStatus::Armed] {
        assert_eq!(
            store.lease_for_sampling(receipt_id, &owner, SamplingSource::PushedCompletion),
            Err(ReceiptError::InvalidTransition {
                actual: expected_status.clone()
            })
        );
        assert_eq!(
            store.fail_sampling(&wrong_phase),
            Err(ReceiptError::StaleLease)
        );
        assert_eq!(
            store.acknowledge_sampled(&wrong_phase),
            Err(ReceiptError::StaleLease)
        );
        assert_eq!(
            store.status(receipt_id, &owner),
            Ok(expected_status.clone())
        );
        if expected_status == ReceiptStatus::Reserved {
            assert_eq!(
                store.resolve_initial_response(receipt_id, &owner, InitialResponseDecision::Arm),
                Ok(InitialResponseOutcome::Armed)
            );
        }
    }
    assert_eq!(store.fail_sampling(&donor), Ok(()));
    assert_eq!(store.fail_sampling(&donor), Err(ReceiptError::StaleLease));
    assert_eq!(
        store.acknowledge_sampled(&donor),
        Err(ReceiptError::StaleLease)
    );
    assert_eq!(store.status(donor_id, &owner), Ok(ReceiptStatus::Queued));
    let live = store
        .lease_for_sampling(donor_id, &owner, SamplingSource::PushedCompletion)
        .expect("requeued receipt should lease");
    let mut wrong_source = live.clone();
    wrong_source.source = SamplingSource::TerminalStdinOutput;
    assert_eq!(
        store.fail_sampling(&wrong_source),
        Err(ReceiptError::StaleLease)
    );
    assert_eq!(
        store.acknowledge_sampled(&wrong_source),
        Err(ReceiptError::StaleLease)
    );
    assert_eq!(
        store.publish_exit(donor_id, &owner, completion(Some(99))),
        Err(ReceiptError::InvalidTransition {
            actual: ReceiptStatus::LeasedToSampling {
                source: SamplingSource::PushedCompletion
            },
        })
    );
    assert_eq!(
        store.status(donor_id, &owner),
        Ok(ReceiptStatus::LeasedToSampling {
            source: SamplingSource::PushedCompletion
        })
    );
    assert_eq!(store.acknowledge_sampled(&live), Ok(completion(Some(0))));
}

#[test]
fn completion_receipt_unknown_id_is_refused_by_every_entry() {
    let store = CompletionReceiptStore::default();
    let owner = receipt_owner(/*runtime_generation*/ 71);
    let donor_id = queued_receipt(&store, &owner);
    let mut unknown_lease = store
        .lease_for_sampling(donor_id, &owner, SamplingSource::PushedCompletion)
        .expect("donor receipt should lease");
    let unknown_id = super::ReceiptId(uuid::Uuid::nil());
    unknown_lease.receipt_id = unknown_id;
    assert_eq!(
        store.status(unknown_id, &owner),
        Err(ReceiptError::UnknownReceipt)
    );
    assert_eq!(
        store.cancel(unknown_id, &owner, CancellationReason::Released),
        Err(ReceiptError::UnknownReceipt)
    );
    assert_eq!(
        store.resolve_initial_response(unknown_id, &owner, InitialResponseDecision::Arm),
        Err(ReceiptError::UnknownReceipt)
    );
    assert_eq!(
        store.publish_exit(unknown_id, &owner, completion(Some(0))),
        Err(ReceiptError::UnknownReceipt)
    );
    assert_eq!(
        store.lease_for_sampling(unknown_id, &owner, SamplingSource::PushedCompletion),
        Err(ReceiptError::UnknownReceipt)
    );
    assert_eq!(
        store.fail_sampling(&unknown_lease),
        Err(ReceiptError::UnknownReceipt)
    );
    assert_eq!(
        store.acknowledge_sampled(&unknown_lease),
        Err(ReceiptError::UnknownReceipt)
    );
    assert_eq!(
        store.status(donor_id, &owner),
        Ok(ReceiptStatus::LeasedToSampling {
            source: SamplingSource::PushedCompletion
        })
    );
}

#[test]
fn completion_receipt_terminal_outcomes_refuse_every_late_operation() {
    let store = CompletionReceiptStore::default();
    let owner = receipt_owner(/*runtime_generation*/ 72);
    let sampled_id = queued_receipt(&store, &owner);
    let sampled_lease = store
        .lease_for_sampling(sampled_id, &owner, SamplingSource::PushedCompletion)
        .expect("sampled receipt should lease");
    assert_eq!(
        store.acknowledge_sampled(&sampled_lease),
        Ok(completion(Some(0)))
    );
    let inline_id = store
        .reserve(owner.clone())
        .expect("inline receipt should reserve");
    assert_eq!(
        store.publish_exit(inline_id, &owner, completion(Some(0))),
        Ok(ExitPublicationOutcome::RetainedUntilDecision)
    );
    assert_eq!(
        store.resolve_initial_response(inline_id, &owner, InitialResponseDecision::InlineResult),
        Ok(InitialResponseOutcome::InlineResult(completion(Some(0))))
    );
    let cancelled_id = queued_receipt(&store, &owner);
    assert_eq!(
        store.cancel(cancelled_id, &owner, CancellationReason::Released),
        Ok(())
    );
    for (receipt_id, status) in [
        (
            sampled_id,
            ReceiptStatus::Sampled {
                source: SamplingSource::PushedCompletion,
            },
        ),
        (inline_id, ReceiptStatus::InlineResult),
        (
            cancelled_id,
            ReceiptStatus::Cancelled {
                reason: CancellationReason::Released,
            },
        ),
    ] {
        let error = || match &status {
            ReceiptStatus::Sampled { .. } => ReceiptError::AlreadyConsumed,
            ReceiptStatus::InlineResult => ReceiptError::InvalidTransition {
                actual: ReceiptStatus::InlineResult,
            },
            ReceiptStatus::Cancelled { reason } => ReceiptError::Cancelled { reason: *reason },
            ReceiptStatus::Reserved
            | ReceiptStatus::Armed
            | ReceiptStatus::Queued
            | ReceiptStatus::LeasedToSampling { .. } => {
                panic!("test only constructs terminal states")
            }
        };
        let mut late_lease = sampled_lease.clone();
        late_lease.receipt_id = receipt_id;
        for decision in [
            InitialResponseDecision::Arm,
            InitialResponseDecision::InlineResult,
        ] {
            assert_eq!(
                store.resolve_initial_response(receipt_id, &owner, decision),
                Err(error())
            );
        }
        assert_eq!(
            store.publish_exit(receipt_id, &owner, completion(Some(99))),
            Err(error())
        );
        assert_eq!(
            store.lease_for_sampling(receipt_id, &owner, SamplingSource::PushedCompletion),
            Err(error())
        );
        assert_eq!(store.fail_sampling(&late_lease), Err(error()));
        assert_eq!(store.acknowledge_sampled(&late_lease), Err(error()));
        assert_eq!(
            store.cancel(receipt_id, &owner, CancellationReason::Shutdown),
            Err(ReceiptError::AlreadyTerminal)
        );
        assert_eq!(store.status(receipt_id, &owner), Ok(status));
    }
}

#[test]
fn completion_receipt_poisoned_lock_returns_a_structured_error_after_panic() {
    let store = Arc::new(CompletionReceiptStore::default());
    let owner = receipt_owner(16);
    let receipt_id = queued_receipt(&store, &owner);
    let lease = store
        .lease_for_sampling(receipt_id, &owner, SamplingSource::PushedCompletion)
        .expect("receipt should lease before the panic");
    let poison_store = Arc::clone(&store);
    let panicker = thread::spawn(move || {
        let _guard = poison_store
            .state
            .lock()
            .expect("initial lock should be clean");
        panic!("poison the receipt lock for this test");
    });

    assert!(
        panicker.join().is_err(),
        "the lock-poisoning panic is expected"
    );
    assert_eq!(
        store.reserve(owner.clone()),
        Err(ReceiptError::LockPoisoned)
    );
    assert_eq!(
        store.status(receipt_id, &owner),
        Err(ReceiptError::LockPoisoned)
    );
    assert_eq!(
        store.resolve_initial_response(receipt_id, &owner, InitialResponseDecision::Arm),
        Err(ReceiptError::LockPoisoned)
    );
    assert_eq!(
        store.publish_exit(receipt_id, &owner, completion(Some(0))),
        Err(ReceiptError::LockPoisoned)
    );
    assert_eq!(
        store.lease_for_sampling(receipt_id, &owner, SamplingSource::PushedCompletion),
        Err(ReceiptError::LockPoisoned)
    );
    assert_eq!(store.fail_sampling(&lease), Err(ReceiptError::LockPoisoned));
    assert_eq!(
        store.acknowledge_sampled(&lease),
        Err(ReceiptError::LockPoisoned)
    );
    assert_eq!(
        store.cancel(receipt_id, &owner, CancellationReason::Released),
        Err(ReceiptError::LockPoisoned)
    );
}

#[test]
fn model_handle_round_trips_and_rejects_non_uuids() {
    let store = CompletionReceiptStore::default();
    let owner = receipt_owner(1);
    let receipt_id = store.reserve(owner).expect("reservation should succeed");
    let handle = receipt_id.model_handle();
    assert_eq!(ReceiptId::from_model_handle(&handle), Some(receipt_id));
    assert_eq!(
        ReceiptId::from_model_handle(&format!("  {handle}\n")),
        Some(receipt_id),
        "surrounding whitespace should be tolerated"
    );
    assert_eq!(ReceiptId::from_model_handle("not-a-receipt"), None);
    assert_eq!(ReceiptId::from_model_handle(""), None);
}
