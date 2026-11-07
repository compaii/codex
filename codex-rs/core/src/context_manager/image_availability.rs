//! Projects unavailable file images into placeholders without changing tool attribution.

use std::collections::HashSet;

use codex_attachment_store::AttachmentStore;
use codex_context_fragments::AnnotatedContent;
use codex_context_fragments::set_annotated_content;
use codex_context_fragments::to_annotated_content;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ContentItemKind;
use codex_protocol::models::ContentItemMetadata;
use codex_protocol::models::FunctionCallOutputContentItem;
use codex_protocol::models::ImageReference;
use codex_protocol::models::ResponseItem;
use futures::StreamExt;
use futures::stream;

const MAX_CONCURRENT_IMAGE_AVAILABILITY_CHECKS: usize = 8;

/// Checks distinct file IDs concurrently and replaces only unavailable image content.
pub(crate) async fn replace_unresolvable_image_files<'a>(
    items: impl Iterator<Item = &'a mut ResponseItem>,
    image_store: &dyn AttachmentStore,
) {
    let items: Vec<_> = items.collect();
    let mut file_ids = HashSet::new();
    for item in &items {
        match item {
            ResponseItem::Message { content, .. } => {
                for part in content {
                    if let ContentItem::InputImage {
                        image: ImageReference::File { file_id },
                        ..
                    } = part
                    {
                        file_ids.insert(file_id.clone());
                    }
                }
            }
            ResponseItem::FunctionCallOutput { output, .. }
            | ResponseItem::CustomToolCallOutput { output, .. } => {
                if let Some(content) = output.content_items() {
                    for part in content {
                        if let FunctionCallOutputContentItem::InputImage {
                            image: ImageReference::File { file_id },
                            ..
                        } = part
                        {
                            file_ids.insert(file_id.clone());
                        }
                    }
                }
            }
            _ => {}
        }
    }
    let mut checks = stream::iter(file_ids)
        .map(|file_id| async move {
            let resolvable = image_store.is_resolvable(&file_id).await;
            (file_id, resolvable)
        })
        .buffer_unordered(MAX_CONCURRENT_IMAGE_AVAILABILITY_CHECKS);
    let mut unavailable_file_ids = HashSet::new();
    while let Some((file_id, resolvable)) = checks.next().await {
        if !resolvable {
            unavailable_file_ids.insert(file_id);
        }
    }
    if unavailable_file_ids.is_empty() {
        return;
    }

    for item in items {
        match item {
            ResponseItem::Message { content, .. } => {
                let mut unavailable = Vec::new();
                for (index, part) in content.iter().enumerate() {
                    if let ContentItem::InputImage {
                        image: ImageReference::File { file_id },
                        ..
                    } = part
                        && unavailable_file_ids.contains(file_id)
                    {
                        unavailable.push(index);
                    }
                }
                if unavailable.is_empty() {
                    continue;
                }
                let Some(mut content) = to_annotated_content(item) else {
                    continue;
                };
                for index in unavailable {
                    content[index] = AnnotatedContent::text(
                        "[image unavailable]",
                        ContentItemKind("images.unavailable".to_string()),
                        ContentItemMetadata::harness(),
                    );
                }
                let _ = set_annotated_content(item, content);
            }
            ResponseItem::FunctionCallOutput { output, .. }
            | ResponseItem::CustomToolCallOutput { output, .. } => {
                if let Some(content) = output.content_items_mut() {
                    for part in content {
                        if let FunctionCallOutputContentItem::InputImage {
                            image: ImageReference::File { file_id },
                            ..
                        } = part
                            && unavailable_file_ids.contains(file_id)
                        {
                            *part = FunctionCallOutputContentItem::InputText {
                                text: "[image unavailable]".to_string(),
                            };
                        }
                    }
                }
            }
            _ => {}
        }
    }
}

/// Prompt-only previews of positively identified older-turn inline images.
/// Unknown provenance and current input are preserved; transport validation still
/// rejects any complete request that cannot fit. No retained item is rewritten.
pub(crate) fn bound_historical_image_previews(
    items: &mut Vec<ResponseItem>,
    current_turn: &str,
    budget: usize,
) {
    use crate::utils::json::serialized_json_bytes;
    let Ok(total) = items.iter().try_fold(0usize, |sum, item| {
        serialized_json_bytes(item).map(|size| sum.saturating_add(size))
    }) else {
        return;
    };
    if total <= budget {
        return;
    }
    let mut count = 0usize;
    let mut image_bytes = 0usize;
    for item in items
        .iter()
        .filter(|item| item.turn_id().is_some_and(|id| id != current_turn))
    {
        for url in historical_inline_urls(item) {
            count += 1;
            image_bytes = image_bytes.saturating_add(url.len());
        }
    }
    if count == 0 {
        return;
    }
    let per_image = budget
        .saturating_sub(total.saturating_sub(image_bytes))
        .saturating_sub(1024)
        / count;
    let mut changed = false;
    for item in items
        .iter_mut()
        .filter(|item| item.turn_id().is_some_and(|id| id != current_turn))
    {
        match item {
            ResponseItem::Message { .. } => {
                let Some(mut content) = to_annotated_content(item) else {
                    continue;
                };
                for part in &mut content {
                    if let ContentItem::InputImage {
                        image: ImageReference::Inline { image_url },
                        ..
                    } = part.content_mut()
                        && image_url.starts_with("data:image/")
                        && image_url.len() > per_image
                    {
                        if let Some(preview) = historical_preview(image_url, per_image) {
                            *image_url = preview;
                        } else {
                            *part = AnnotatedContent::text(
                                "[historical image omitted from bounded request; original retained]",
                                ContentItemKind("images.bounded_preview".into()),
                                ContentItemMetadata::harness(),
                            );
                        }
                        changed = true;
                    }
                }
                let _ = set_annotated_content(item, content);
            }
            ResponseItem::FunctionCallOutput { output, .. }
            | ResponseItem::CustomToolCallOutput { output, .. } => {
                if let Some(content) = output.content_items_mut() {
                    for part in content {
                        if let FunctionCallOutputContentItem::InputImage {
                            image: ImageReference::Inline { image_url },
                            ..
                        } = part
                            && image_url.starts_with("data:image/")
                            && image_url.len() > per_image
                        {
                            if let Some(preview) = historical_preview(image_url, per_image) {
                                *image_url = preview;
                            } else {
                                *part = FunctionCallOutputContentItem::InputText {text:"[historical image omitted from bounded request; original retained]".into()};
                            }
                            changed = true;
                        }
                    }
                }
            }
            _ => {}
        }
    }
    if changed {
        let mut note = codex_context_fragments::message_from_parts(
            "developer",
            vec![AnnotatedContent::text(
                "Harness observation: historical images in this bounded request use reduced previews or explicit omission placeholders. Originals remain in native history. Current-turn input and all text were preserved.",
                ContentItemKind("images.bounded_preview".into()),
                ContentItemMetadata::harness(),
            )],
        );
        note.set_turn_id_if_missing(current_turn);
        // Foreground V2 compaction deliberately ends with its trigger. The
        // request-only annotation must preserve that control-item position.
        if matches!(items.last(), Some(ResponseItem::CompactionTrigger { .. })) {
            items.insert(items.len() - 1, note);
        } else {
            items.push(note);
        }
    }
}

fn historical_inline_urls(item: &ResponseItem) -> Vec<&str> {
    match item {
        ResponseItem::Message { content, .. } => content
            .iter()
            .filter_map(|part| {
                if let ContentItem::InputImage {
                    image: ImageReference::Inline { image_url },
                    ..
                } = part
                {
                    image_url
                        .starts_with("data:image/")
                        .then_some(image_url.as_str())
                } else {
                    None
                }
            })
            .collect(),
        ResponseItem::FunctionCallOutput { output, .. }
        | ResponseItem::CustomToolCallOutput { output, .. } => output
            .content_items()
            .into_iter()
            .flatten()
            .filter_map(|part| {
                if let FunctionCallOutputContentItem::InputImage {
                    image: ImageReference::Inline { image_url },
                    ..
                } = part
                {
                    image_url
                        .starts_with("data:image/")
                        .then_some(image_url.as_str())
                } else {
                    None
                }
            })
            .collect(),
        _ => vec![],
    }
}

fn historical_preview(url: &str, max_bytes: usize) -> Option<String> {
    use codex_utils_image::PromptImageMode;
    use codex_utils_image::PromptImageResizeLimits;
    use codex_utils_image::load_data_url_for_prompt;
    for size in [1024u32, 512, 256, 128, 64] {
        let mode = PromptImageMode::ResizeWithLimits(PromptImageResizeLimits {
            max_dimension: size,
            max_patches: (size as usize / 32).pow(2),
        });
        let Ok(image) = load_data_url_for_prompt(url, mode) else {
            return None;
        };
        let preview = image.into_data_url();
        if preview.len() <= max_bytes {
            return Some(preview);
        }
    }
    None
}

#[cfg(test)]
mod bounded_preview_tests {
    use super::*;
    use std::io::Cursor;

    fn image_url() -> String {
        let image = image::RgbImage::from_fn(512, 512, |x, y| {
            let n = x.wrapping_mul(0x9e3779b9) ^ y.wrapping_mul(0x85ebca6b);
            let n = (n ^ (n >> 16)).wrapping_mul(0x7feb352d);
            image::Rgb([n as u8, (n >> 8) as u8, (n >> 16) as u8])
        });
        let mut bytes = Cursor::new(Vec::new());
        image.write_to(&mut bytes, image::ImageFormat::Png).unwrap();
        codex_utils_image::data_url_from_bytes("image/png", bytes.get_ref())
    }
    fn message(url: &str, turn: Option<&str>) -> ResponseItem {
        let mut item: ResponseItem=serde_json::from_value(serde_json::json!({"type":"message","role":"user","content":[{"type":"input_text","text":"Keep this exact human text"},{"type":"input_image","image_url":url}]})).unwrap();
        if let Some(turn) = turn {
            item.set_turn_id_if_missing(turn)
        }
        item
    }
    #[test]
    fn bounded_previews_preserve_retained_history_current_input_and_text() {
        let url = image_url();
        let original = vec![
            message(&url, Some("old-1")),
            message(&url, Some("old-2")),
            message(&url, Some("old-3")),
            message(&url, Some("current")),
        ];
        let mut projected = original.clone();
        let budget = 2 * 1024 * 1024;
        assert!(crate::utils::json::serialized_json_bytes(&projected).unwrap() > budget);
        bound_historical_image_previews(&mut projected, "current", budget);
        assert!(crate::utils::json::serialized_json_bytes(&projected).unwrap() < budget);
        assert_eq!(projected[3], original[3]);
        assert_eq!(original[0], message(&url, Some("old-1")));
        for item in &projected[..3] {
            assert_ne!(historical_inline_urls(item), vec![url.as_str()]);
            let ResponseItem::Message { content, .. } = item else {
                panic!("message")
            };
            assert_eq!(
                content[0],
                codex_protocol::models::input_text("Keep this exact human text")
            );
        }
        let ResponseItem::Message { role, .. } = projected.last().unwrap() else {
            panic!("harness note")
        };
        assert_eq!(role, "developer");
    }
    #[test]
    fn bounded_previews_never_guess_current_or_unknown_image_provenance() {
        let url = image_url();
        let original = vec![message(&url, None), message(&url, Some("current"))];
        let mut projected = original.clone();
        bound_historical_image_previews(&mut projected, "current", 128);
        assert_eq!(projected, original);
        let mut projected = vec![message(&url, Some("old"))];
        let original = projected.clone();
        bound_historical_image_previews(&mut projected, "current", usize::MAX);
        assert_eq!(projected, original);
    }

    #[test]
    fn bounded_previews_preserve_final_compaction_trigger() {
        let url = format!("data:image/png;base64,{}", "x".repeat(4096));
        let mut items = vec![
            message(&url, Some("old")),
            ResponseItem::CompactionTrigger {},
        ];
        bound_historical_image_previews(&mut items, "current", 1024);
        assert_eq!(items.last(), Some(&ResponseItem::CompactionTrigger {}));
        assert!(matches!(&items[1], ResponseItem::Message {role,..} if role=="developer"));
    }

    /// Explicit read-only qualification of a selected owner's retained history.
    /// Never runs against private history in CI or makes a provider request.
    #[test]
    #[ignore = "requires an explicitly selected retained history file"]
    fn bounded_previews_selected_retained_history() {
        use std::io::BufRead;
        let path = std::env::var("CODEX_BOUNDED_HISTORY_QUALIFICATION").unwrap();
        let file = std::fs::File::open(path).unwrap();
        assert!(file.metadata().unwrap().len() <= 512 * 1024 * 1024);
        let mut items = Vec::new();
        for line in std::io::BufReader::new(file).lines() {
            let value: serde_json::Value = serde_json::from_str(&line.unwrap()).unwrap();
            if value["type"] == "compacted" {
                if let Some(replacement) = value["payload"]["replacement_history"].as_array() {
                    items = replacement
                        .iter()
                        .cloned()
                        .map(|item| serde_json::from_value(item).unwrap())
                        .collect();
                }
            } else if value["type"] == "response_item" {
                items.push(serde_json::from_value(value["payload"].clone()).unwrap());
            }
        }
        let before = crate::utils::json::serialized_json_bytes(&items).unwrap();
        let text_before: Vec<_> = items
            .iter()
            .flat_map(|item| match item {
                ResponseItem::Message { content, .. } => content
                    .iter()
                    .filter_map(|part| match part {
                        ContentItem::InputText { text } => Some(text.clone()),
                        _ => None,
                    })
                    .collect(),
                _ => Vec::new(),
            })
            .collect();
        let current = std::env::var("CODEX_BOUNDED_HISTORY_CURRENT_TURN").unwrap();
        bound_historical_image_previews(&mut items, &current, 4 * 1024 * 1024);
        let after = crate::utils::json::serialized_json_bytes(&items).unwrap();
        let text_after: Vec<_> = items
            .iter()
            .flat_map(|item| match item {
                ResponseItem::Message { content, .. } => content
                    .iter()
                    .filter_map(|part| match part {
                        ContentItem::InputText { text }
                            if !text.starts_with("Harness observation:") =>
                        {
                            Some(text.clone())
                        }
                        _ => None,
                    })
                    .collect(),
                _ => Vec::new(),
            })
            .collect();
        assert!(text_before == text_after, "retained text changed");
        assert!(before > 8 * 1024 * 1024);
        assert!(after < 4 * 1024 * 1024);
        println!(
            "Selected retained input projected: before_bytes={before}, after_bytes={after}; original file read-only; zero provider requests"
        );
    }
}
