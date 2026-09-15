use super::with_file_url_references;
use pretty_assertions::assert_eq;

#[test]
fn modern_file_links_use_file_urls_without_rewriting_other_guidance() {
    let instructions = "# Formatting\n\n- You may format with GitHub-flavored Markdown.\n- When referencing a real local file, prefer a clickable markdown link.\n  * Clickable file links should look like [app.py](/abs/path/app.py:12).\n  * Do not use URIs like file://, vscode://, or https:// for file links.\n\nCite web sources as [Source](https://example.com).\n";
    let rendered = with_file_url_references(instructions);

    assert!(rendered.contains("[app.py](file:///abs/path/app.py)"));
    assert!(rendered.contains("file:///abs/path/My%20Project/My%20Report.md"));
    assert!(!rendered.contains("Do not use URIs"));
    assert!(!rendered.contains("[app.py](/abs/"));
    assert!(
        rendered.starts_with("# Formatting\n\n- You may format with GitHub-flavored Markdown.\n")
    );
    assert!(rendered.ends_with("\nCite web sources as [Source](https://example.com).\n"));
    assert_eq!(with_file_url_references(&rendered), rendered);
}

#[test]
fn legacy_file_reference_section_uses_the_same_url_guidance() {
    let instructions = "Before.\n\n**File References**\nWhen referencing files in your response, follow these rules:\n  * Use inline code to make file paths clickable.\n  * Do not use URIs like file://, vscode://, or https://.\n\n**Structure**\nKeep related bullets together.\n";
    let rendered = with_file_url_references(instructions);

    assert!(rendered.contains("[app.py](file:///abs/path/app.py)"));
    assert!(!rendered.contains("Use inline code to make file paths clickable"));
    assert!(!rendered.contains("Do not use URIs"));
    assert!(rendered.starts_with("Before.\n\n**File References**\n"));
    assert!(rendered.ends_with("\n**Structure**\nKeep related bullets together.\n"));
}

#[test]
fn instructions_without_vendor_file_reference_sections_remain_unchanged() {
    let instructions = "Read file:///tmp/input.txt.\nUse [API docs](https://example.com).\n";
    assert_eq!(with_file_url_references(instructions), instructions);
}
