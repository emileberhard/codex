const FILE_REFERENCE_GUIDANCE: &str = "- Format local-file references as Markdown links with absolute `file:///` URLs, for example `[app.py](file:///abs/path/app.py)`.\n  * Encode spaces in URL targets as `%20`, for example `[My Report.md](file:///abs/path/My%20Project/My%20Report.md)`.\n  * Keep link labels readable and do not wrap Markdown links in backticks.\n";

/// Replaces known vendor file-link rules in the request copy of model-owned instructions.
/// Caller-authored instructions and persisted history must not pass through this adapter.
pub fn with_file_url_references(instructions: &str) -> String {
    let mut lines = instructions.split_inclusive('\n').peekable();
    let mut rendered = String::with_capacity(instructions.len());
    while let Some(line) = lines.next() {
        match line.trim_end() {
            "- When referencing a real local file, prefer a clickable markdown link." => {
                rendered.push_str(FILE_REFERENCE_GUIDANCE);
                while lines.peek().is_some_and(|line| line.starts_with("  ")) {
                    lines.next();
                }
            }
            "**File References**" => {
                rendered.push_str("**File References**\n");
                rendered.push_str(FILE_REFERENCE_GUIDANCE);
                while lines.peek().is_some_and(|line| !line.trim().is_empty()) {
                    lines.next();
                }
            }
            _ => rendered.push_str(line),
        }
    }
    rendered
}

#[cfg(test)]
#[path = "file_reference_instructions_tests.rs"]
mod tests;
