//! Verifies prefix placement and item boundaries in initial session context assembly.

use super::step_context::StepContext;
use super::tests::make_session_and_context;
use crate::context::BaseInstructionsFragment;
use crate::context::ContextualUserFragment;
use crate::context::DeveloperInstructions;
use crate::context::UserInstructions;
use crate::context::world_state::Placement;
use crate::context::world_state::PreviousSectionState;
use crate::context::world_state::SectionTransition;
use crate::context::world_state::WorldState;
use crate::context::world_state::WorldStateSection;
use crate::context::world_state::WorldStateUpdate;
use crate::context::world_state::WorldStateUpdateContent;
use crate::context::world_state::split_prefix_updates;
use crate::context_manager::updates::merge_world_state_updates;
use codex_protocol::models::ResponseItem;
use core_test_support::responses::strip_metadata_from_items;
use pretty_assertions::assert_eq;
use serde_json::json;
use std::sync::Arc;

struct ItemSection;

impl WorldStateSection for ItemSection {
    const ID: &'static str = "items";
    type Snapshot = bool;

    fn render_diff(
        &self,
        _: PreviousSectionState<'_, Self::Snapshot>,
    ) -> SectionTransition<Self::Snapshot> {
        (
            Some(true),
            vec![
                WorldStateUpdate {
                    placement: Placement::Prefix,
                    content: WorldStateUpdateContent::Item(Box::new(additional_tools("example"))),
                },
                WorldStateUpdate {
                    placement: Placement::Prefix,
                    ..WorldStateUpdate::fragment(BaseInstructionsFragment(
                        "base instructions".to_string(),
                    ))
                },
                WorldStateUpdate::fragment(DeveloperInstructions::new("before item")),
                WorldStateUpdate::fragment(UserInstructions {
                    directory: None,
                    text: "user before item".to_string(),
                }),
                WorldStateUpdate::fragment(DeveloperInstructions::new("separate before item"))
                    .standalone(),
                WorldStateUpdate {
                    placement: Placement::Standalone,
                    content: WorldStateUpdateContent::Item(Box::new(additional_tools("middle"))),
                },
                WorldStateUpdate {
                    placement: Placement::Mergeable,
                    content: WorldStateUpdateContent::Item(Box::new(additional_tools("adjacent"))),
                },
                WorldStateUpdate::fragment(DeveloperInstructions::new("after item")),
                WorldStateUpdate::fragment(UserInstructions {
                    directory: None,
                    text: "user after item".to_string(),
                }),
                WorldStateUpdate {
                    placement: Placement::Standalone,
                    content: WorldStateUpdateContent::Item(Box::new(additional_tools("trailing"))),
                },
            ],
        )
    }
}

fn additional_tools(name: &str) -> ResponseItem {
    ResponseItem::AdditionalTools {
        id: None,
        role: "developer".to_string(),
        tools: vec![json!({"type": "function", "name": name})],
    }
}

#[tokio::test]
async fn initial_context_preserves_world_state_items_and_snapshot() {
    let (session, turn_context) = make_session_and_context().await;
    let step_context = StepContext::for_test(Arc::new(turn_context));
    let mut world_state = WorldState::default();
    world_state.add_section(ItemSection);
    let (updates, snapshot) = session
        .build_initial_context_with_world_state(&step_context, &world_state)
        .await;
    let (prefix, context) = split_prefix_updates(updates);
    assert_eq!(
        strip_metadata_from_items(&prefix),
        strip_metadata_from_items(&[
            additional_tools("example"),
            ContextualUserFragment::into(BaseInstructionsFragment("base instructions".to_string())),
        ]),
    );
    assert_eq!(
        strip_metadata_from_items(&merge_world_state_updates(context)),
        strip_metadata_from_items(&[
            ContextualUserFragment::into(DeveloperInstructions::new("before item")),
            ContextualUserFragment::into(DeveloperInstructions::new("separate before item")),
            ContextualUserFragment::into(UserInstructions {
                directory: None,
                text: "user before item".to_string(),
            }),
            additional_tools("middle"),
            additional_tools("adjacent"),
            ContextualUserFragment::into(DeveloperInstructions::new("after item")),
            ContextualUserFragment::into(UserInstructions {
                directory: None,
                text: "user after item".to_string(),
            }),
            additional_tools("trailing"),
        ]),
    );
    assert_eq!(
        serde_json::to_value(snapshot).unwrap(),
        json!({"items": true})
    );
}

#[tokio::test]
async fn oversized_base_instructions_refuse_before_world_state_write() {
    let (mut session, mut turn_context) = make_session_and_context().await;
    let store = super::tests::attach_in_memory_thread_store(&mut session).await;
    Arc::make_mut(&mut turn_context.config)
        .features
        .enable(codex_features::Feature::IncrementalTools);
    let mut step = StepContext::for_test(Arc::new(turn_context));
    Arc::make_mut(&mut Arc::make_mut(&mut Arc::get_mut(&mut step).unwrap().settings).model_info)
        .use_responses_lite = true;
    assert!(step.uses_incremental_tools());
    session
        .state
        .lock()
        .await
        .session_configuration
        .base_instructions = "x".repeat(40_001);
    let before = store.calls().await;
    let result = session.record_step_world_state_if_changed(&step).await;
    assert!(
        result.is_err(),
        "oversized base instructions were recorded in world state"
    );
    assert!(
        result
            .err()
            .unwrap()
            .to_string()
            .contains("10001 estimated tokens")
    );
    assert_eq!(store.calls().await, before);
    let initial = session
        .record_context_updates_and_set_reference_context_item(&step)
        .await;
    assert!(initial.is_err());
    assert!(
        initial
            .err()
            .unwrap()
            .to_string()
            .contains("10001 estimated tokens")
    );
    assert_eq!(store.calls().await, before);

    assert!(
        session
            .state
            .lock()
            .await
            .history
            .raw_items()
            .next()
            .is_none()
    );
    assert_eq!(
        session.get_base_instructions().await.text,
        "x".repeat(40_001)
    );
    session
        .state
        .lock()
        .await
        .session_configuration
        .base_instructions = "é".repeat(20_000);
    let world = session
        .record_step_world_state_if_changed(&step)
        .await
        .unwrap();
    let (_, updates) = world.render_full();
    let (prefix, _) = split_prefix_updates(updates);
    let instruction = prefix
        .iter()
        .find(|item| BaseInstructionsFragment::matches_item(item))
        .unwrap();
    let expected = ContextualUserFragment::into(BaseInstructionsFragment("é".repeat(20_000)));
    assert_eq!(
        strip_metadata_from_items(&[instruction.clone()]),
        strip_metadata_from_items(&[expected])
    );
    assert_eq!(
        session.get_base_instructions().await.text,
        "é".repeat(20_000)
    );
}
