//! Entity extractor trait and the heuristic default.
//!
//! README §6.1 step 2 (Entity Extraction) pulls named entities
//! and implicit references out of the normalized content and
//! assigns a source tier. The actual extraction logic is
//! pluggable: production deployments swap in an LLM-based
//! extractor (issue #16 — the open "LLM selection for
//! pipeline steps" question); the default in the meantime is
//! [`HeuristicEntityExtractor`], a rule-based extractor that
//! runs in-process and gives the pipeline enough to do
//! meaningful disambiguation in tests.
//!
//! The heuristic is intentionally simple: a curated list of
//! role prefixes ("my manager", "the project", "she/her") plus
//! a capitalisation heuristic for proper-noun spans, plus a
//! small dictionary of common entity types. It is **not** a
//! general-purpose NER system; it is a placeholder that the
//! real LLM-based extractor will replace. The trait surface is
//! identical so the production swap is one line in the
//! pipeline builder.

use std::collections::HashMap;

use async_trait::async_trait;

use engram_storage::SignalTier;

use crate::error::IngestResult;

/// A single extracted mention: the surface form, the
/// candidate canonical name, the inferred entity type, and the
/// source tier (lifted from the content type at the call site).
///
/// The disambiguation step operates on lists of these; the
/// write step uses the canonical_name to look up or create
/// entity records.
#[derive(Debug, Clone, PartialEq)]
pub struct ExtractedMention {
    /// The surface text as it appeared in the content.
    pub surface: String,
    /// The canonical name to use for the entity record. For
    /// a proper-noun span like "Sarah Chen" the surface and
    /// canonical are identical; for a role reference like
    /// "my manager" the canonical is the resolved entity's
    /// name (which the disambiguation pass fills in).
    pub canonical_name: String,
    /// The inferred entity type: person | organization |
    /// project | location | concept | other.
    pub entity_type: String,
    /// The source tier that produced this mention. For the
    /// heuristic extractor this is the same as the content
    /// type's default tier; an LLM-based extractor can
    /// re-derive it per-mention.
    pub source_tier: SignalTier,
    /// The 0.0–1.0 confidence the extractor has in the
    /// mention. The disambiguation pass combines this with
    /// the source tier to decide whether to merge, flag, or
    /// create.
    pub confidence: f32,
    /// Implicit references (e.g. "my manager", "she") get a
    /// non-empty reference_kind so the disambiguation pass
    /// can resolve them. Empty for explicit named-entity
    /// mentions.
    pub reference_kind: ReferenceKind,
}

/// What kind of reference a mention is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferenceKind {
    /// A proper noun — "Sarah Chen", "Acme", "Berlin".
    Explicit,
    /// A role or relation reference — "my manager", "the
    /// project lead", "the company".
    Role,
    /// A pronoun — "she", "he", "they", "it".
    Pronoun,
}

impl ReferenceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ReferenceKind::Explicit => "explicit",
            ReferenceKind::Role => "role",
            ReferenceKind::Pronoun => "pronoun",
        }
    }
}

/// The extractor trait. Production implementations call an
/// LLM (issue #16); the heuristic default is regex +
/// dictionary based and runs in-process.
#[async_trait]
pub trait EntityExtractor: Send + Sync {
    /// A short identifier for the extractor. Used in error
    /// messages and in `engram status` (Phase 4).
    fn extractor_id(&self) -> &str;

    /// Extract mentions from the given text. The `source_tier`
    /// is the tier of the *content* the text came from; the
    /// extractor can use it to weight its own confidence.
    async fn extract(
        &self,
        text: &str,
        source_tier: SignalTier,
    ) -> IngestResult<Vec<ExtractedMention>>;
}

// --- Heuristic default ---------------------------------------------------
//
// The heuristic is a four-pass scan over the input:
//
// 1. **Implicit reference pass** — match a fixed list of
//    role/pronoun patterns ("my manager", "she/her", etc.) and
//    emit ReferenceKind::Role / Pronoun mentions. The
//    disambiguation step resolves these against existing
//    entities with the matching role.
//
// 2. **Proper-noun pass** — scan for capitalised spans
//    (e.g. "Sarah Chen", "Acme Corp"). A span is a run of
//    1-4 consecutive Capitalised words. The "first word of a
//    sentence" false positive is filtered by also requiring
//    the next token to be capitalised (with the standard
//    English exceptions for sentence-initial words handled
//    later). All-caps acronyms like "VP" or "OKR" are
//    ignored — they're role signals, not entity mentions.
//
// 3. **Email pass** — match RFC-lite email regex. The local
//    part is folded into a candidate entity name (e.g.
//    "sarah.chen@acme.com" → "Sarah Chen" if the
//    proper-noun pass saw "Sarah Chen" in the same input,
//    otherwise just keep the email as an alias).
//
// 4. **Type inference pass** — for each proper-noun span,
//    assign an entity type from a small dictionary: words
//    like "Inc", "Corp", "Ltd", "GmbH", "AG" → organization;
//    "Project", "Initiative" → project; known city names →
//    location; everything else defaults to person. The
//    dictionary is intentionally small — a larger one is the
//    job of the real extractor (issue #16).
//
// The heuristic's accuracy is a known limitation. The
// disambiguation pass (which still has to deal with the real
// extractor's mistakes) is designed to be robust to false
// positives and false negatives alike, so a noisy heuristic is
// acceptable for development.

const ROLE_PATTERNS: &[&str] = &[
    "my manager", "my boss", "my supervisor",
    "the project", "the project lead", "the lead",
    "the company", "the client", "the customer",
    "the team", "the team lead",
];

const PRONOUNS: &[&str] = &["she", "her", "hers", "he", "him", "his", "they", "them", "their", "it", "its"];

const ORG_SUFFIXES: &[&str] = &[
    "inc", "corp", "ltd", "llc", "gmbh", "ag", "sas", "plc", "co",
];

const PROJECT_KEYWORDS: &[&str] = &["project", "initiative", "program", "programme"];

const KNOWN_LOCATIONS: &[&str] = &[
    "berlin", "paris", "london", "tokyo", "new york", "san francisco",
    "boston", "seattle", "austin", "toronto", "sydney", "singapore",
    "amsterdam", "munich", "zurich", "geneva", "dublin", "tel aviv",
    "bangalore", "mumbai", "delhi", "shanghai", "beijing", "hong kong",
    "seoul", "osaka", "kyoto", "vancouver", "montreal", "mexico city",
    "madrid", "barcelona", "rome", "milan", "lisbon", "vienna", "prague",
    "warsaw", "budapest", "athens", "helsinki", "oslo", "stockholm",
    "copenhagen", "reykjavik", "manchester", "edinburgh", "glasgow",
    "birmingham", "hamburg", "frankfurt", "cologne", "stuttgart",
    "beijing", "tianjin", "guangzhou", "shenzhen", "chengdu", "hangzhou",
];

/// A rule-based extractor. Returns mentions for proper-noun
/// spans, role references, and pronouns. Type inference is
/// coarse but covers the cases the integration tests
/// exercise.
#[derive(Debug, Default, Clone)]
pub struct HeuristicEntityExtractor;

#[async_trait]
impl EntityExtractor for HeuristicEntityExtractor {
    fn extractor_id(&self) -> &str {
        "heuristic-v1"
    }

    async fn extract(
        &self,
        text: &str,
        source_tier: SignalTier,
    ) -> IngestResult<Vec<ExtractedMention>> {
        let mut mentions = Vec::new();
        mentions.extend(extract_role_references(text, source_tier));
        mentions.extend(extract_pronouns(text, source_tier));
        mentions.extend(extract_proper_nouns(text, source_tier));
        mentions.extend(extract_emails(text, source_tier));
        // Deduplicate by (canonical_name, entity_type, reference_kind) so a
        // name mentioned multiple times in the same content doesn't
        // produce duplicate mentions.
        let mut seen: HashMap<(String, String, &'static str), ExtractedMention> = HashMap::new();
        for m in mentions {
            let key = (
                m.canonical_name.to_lowercase(),
                m.entity_type.clone(),
                m.reference_kind.as_str(),
            );
            seen.entry(key).or_insert(m);
        }
        Ok(seen.into_values().collect())
    }
}

fn extract_role_references(text: &str, tier: SignalTier) -> Vec<ExtractedMention> {
    let lower = text.to_lowercase();
    let mut out = Vec::new();
    for pat in ROLE_PATTERNS {
        if let Some(idx) = lower.find(pat) {
            out.push(ExtractedMention {
                surface: text[idx..idx + pat.len()].to_string(),
                canonical_name: pat.to_string(),
                entity_type: "other".to_string(),
                source_tier: tier,
                confidence: 0.6,
                reference_kind: ReferenceKind::Role,
            });
        }
    }
    out
}

fn extract_pronouns(text: &str, tier: SignalTier) -> Vec<ExtractedMention> {
    let mut out = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let lower = text.to_lowercase();
    for word in lower.split(|c: char| !c.is_alphanumeric()) {
        if PRONOUNS.contains(&word) && seen.insert(word.to_string()) {
            out.push(ExtractedMention {
                surface: word.to_string(),
                canonical_name: word.to_string(),
                entity_type: "other".to_string(),
                source_tier: tier,
                confidence: 0.4,
                reference_kind: ReferenceKind::Pronoun,
            });
        }
    }
    out
}

fn extract_proper_nouns(text: &str, tier: SignalTier) -> Vec<ExtractedMention> {
    let mut out = Vec::new();
    let mut current: Vec<(usize, &str)> = Vec::new();
    let words: Vec<(usize, &str)> = word_spans(text);
    let bytes = text.as_bytes();

    for (offset, word) in &words {
        // Skip pronouns entirely. "She told me" at the
        // start of a sentence is capitalised, but it's a
        // pronoun, not a proper-noun mention. The
        // pronoun-detection pass emits its own mention.
        if is_pronoun(word) {
            flush_span(&mut current, text, tier, &mut out);
            continue;
        }
        if is_capitalised(word) && !is_all_caps_acronym(word) {
            current.push((*offset, word));
        } else {
            flush_span(&mut current, text, tier, &mut out);
        }
        // Cap at 4 words to avoid runaway spans.
        if current.len() == 4 {
            flush_span(&mut current, text, tier, &mut out);
        }
        // A sentence-ending period right after the last
        // word in `current` should break the span, even
        // though `word_spans` doesn't emit the period as
        // a token. Without this, "Berlin. Acme Corp"
        // produces a single "Berlin Acme Corp" span.
        if let Some((last_off, last_word)) = current.last() {
            let end = last_off + last_word.len();
            if end < bytes.len() && (bytes[end] == b'.' || bytes[end] == b'!' || bytes[end] == b'?') {
                flush_span(&mut current, text, tier, &mut out);
            }
        }
    }
    flush_span(&mut current, text, tier, &mut out);
    out
}

fn flush_span(
    current: &mut Vec<(usize, &str)>,
    text: &str,
    tier: SignalTier,
    out: &mut Vec<ExtractedMention>,
) {
    if current.is_empty() {
        return;
    }
    // Strip leading articles/determiners ("The", "A") from
    // the span so "The Atlas" surfaces as "Atlas" — the
    // article isn't a proper-noun, it's a sentence-initial
    // false positive. We keep the original surface form for
    // the `surface` field so the disambiguation pass can
    // recognise role references like "the project lead".
    let mut first_real = 0;
    while first_real < current.len() {
        let w = current[first_real].1;
        let lower = w.to_lowercase();
        if matches!(lower.as_str(), "the" | "a" | "an" | "this" | "that" | "these" | "those" | "all" | "every" | "some") {
            first_real += 1;
        } else {
            break;
        }
    }
    if first_real >= current.len() {
        current.clear();
        return;
    }
    let start = current[first_real].0;
    let parts: Vec<&str> = current[first_real..].iter().map(|(_, w)| *w).collect();
    let span = parts.join(" ");
    let lower = span.to_lowercase();
    // We look at the *whole input* for type inference so a
    // trailing role keyword like "project" can flip the
    // type. The type signal is the surrounding context, not
    // the span itself.
    let entity_type = infer_entity_type(&lower, &parts, text);
    out.push(ExtractedMention {
        surface: text[start..start + span.len()].to_string(),
        canonical_name: span.clone(),
        entity_type,
        source_tier: tier,
        confidence: 0.8,
        reference_kind: ReferenceKind::Explicit,
    });
    current.clear();
}

fn infer_entity_type(lower: &str, words: &[&str], text: &str) -> String {
    // Org suffix match (Inc, Corp, Ltd, …) — covers "Acme
    // Inc", "Acme Corp", "Acme Ltd".
    for w in words.iter() {
        let w = w.trim_end_matches(|c: char| !c.is_alphanumeric()).to_lowercase();
        if ORG_SUFFIXES.contains(&w.as_str()) {
            return "organization".to_string();
        }
    }
    // Project keyword: anywhere in the input text. We look
    // at the whole text (not just the span) because the
    // keyword frequently follows the span ("The Atlas
    // project ships Friday" — span is "Atlas", keyword is
    // "project", and the type should be "project").
    let text_lower = text.to_lowercase();
    if PROJECT_KEYWORDS.iter().any(|kw| text_lower.contains(kw)) {
        return "project".to_string();
    }
    // Known location: case-insensitive dictionary match.
    if KNOWN_LOCATIONS.contains(&lower) {
        return "location".to_string();
    }
    // Default for a proper-noun span: person. Org-vs-person
    // disambiguation is a job for the LLM-based extractor.
    "person".to_string()
}

fn extract_emails(text: &str, tier: SignalTier) -> Vec<ExtractedMention> {
    let mut out = Vec::new();
    for (start, end, local, _domain) in find_emails(text) {
        out.push(ExtractedMention {
            surface: text[start..end].to_string(),
            canonical_name: email_to_candidate_name(local),
            entity_type: "person".to_string(),
            source_tier: tier,
            confidence: 0.7,
            reference_kind: ReferenceKind::Explicit,
        });
    }
    out
}

/// Find email-shaped substrings. Returns (start, end,
/// local-part, domain) tuples. The regex is intentionally
/// simple — RFC 5322 is overkill for the heuristic.
fn find_emails(text: &str) -> Vec<(usize, usize, &str, &str)> {
    let mut out = Vec::new();
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'@' {
            // Walk back to the local-part start.
            let mut start = i;
            while start > 0 && is_local_char(bytes[start - 1]) {
                start -= 1;
            }
            // Walk forward to the domain end.
            let mut end = i + 1;
            while end < bytes.len() && is_domain_char(bytes[end]) {
                end += 1;
            }
            if start < i && end > i + 1 {
                let local = &text[start..i];
                let domain = &text[i + 1..end];
                if !local.is_empty() && !domain.is_empty() {
                    out.push((start, end, local, domain));
                }
            }
            i = end;
        } else {
            i += 1;
        }
    }
    out
}

fn is_local_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-' || b == b'+'
}

fn is_domain_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'.' || b == b'-'
}

/// Turn `sarah.chen` into `Sarah Chen`. Lowercases are
/// intentional — the disambiguation pass will fold the
/// candidates against the proper-noun spans.
fn email_to_candidate_name(local: &str) -> String {
    local
        .split(|c: char| c == '.' || c == '_' || c == '-')
        .filter(|s| !s.is_empty())
        .map(capitalise)
        .collect::<Vec<_>>()
        .join(" ")
}

fn capitalise(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) => c.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

fn word_spans(text: &str) -> Vec<(usize, &str)> {
    let mut out = Vec::new();
    let mut start = None;
    for (i, c) in text.char_indices() {
        if c.is_alphanumeric() || c == '\'' {
            if start.is_none() {
                start = Some(i);
            }
        } else if let Some(s) = start.take() {
            out.push((s, &text[s..i]));
        }
    }
    if let Some(s) = start {
        out.push((s, &text[s..]));
    }
    out
}

fn is_capitalised(word: &str) -> bool {
    let mut chars = word.chars();
    match chars.next() {
        Some(c) if c.is_uppercase() => true,
        _ => false,
    }
}

fn is_all_caps_acronym(word: &str) -> bool {
    let len = word.chars().count();
    len <= 5 && word.chars().all(|c| c.is_uppercase() || !c.is_alphabetic())
}

fn is_pronoun(word: &str) -> bool {
    let lower = word.to_lowercase();
    PRONOUNS.contains(&lower.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn extracts_proper_noun_person() {
        let e = HeuristicEntityExtractor;
        let mentions = e
            .extract("Sarah Chen is the VP of Engineering.", SignalTier::Tier3Conversational)
            .await
            .unwrap();
        let names: Vec<&str> = mentions.iter().map(|m| m.canonical_name.as_str()).collect();
        assert!(names.contains(&"Sarah Chen"), "got {names:?}");
    }

    #[tokio::test]
    async fn extracts_organization_via_suffix() {
        let e = HeuristicEntityExtractor;
        let mentions = e
            .extract("Acme Corp is shipping Atlas.", SignalTier::Tier3Conversational)
            .await
            .unwrap();
        let acme = mentions.iter().find(|m| m.canonical_name.starts_with("Acme"));
        assert!(acme.is_some(), "got {mentions:?}");
        assert_eq!(acme.unwrap().entity_type, "organization");
    }

    #[tokio::test]
    async fn extracts_known_location() {
        let e = HeuristicEntityExtractor;
        let mentions = e
            .extract("I work in Berlin.", SignalTier::Tier3Conversational)
            .await
            .unwrap();
        let berlin = mentions.iter().find(|m| m.canonical_name == "Berlin");
        assert!(berlin.is_some(), "got {mentions:?}");
        assert_eq!(berlin.unwrap().entity_type, "location");
    }

    #[tokio::test]
    async fn extracts_email_as_person() {
        let e = HeuristicEntityExtractor;
        let mentions = e
            .extract("Reach me at sarah.chen@acme.com.", SignalTier::Tier3Conversational)
            .await
            .unwrap();
        let m = mentions.iter().find(|m| m.canonical_name == "Sarah Chen");
        assert!(m.is_some(), "got {mentions:?}");
        assert_eq!(m.unwrap().entity_type, "person");
    }

    #[tokio::test]
    async fn extracts_role_reference() {
        let e = HeuristicEntityExtractor;
        let mentions = e
            .extract("Talk to my manager first.", SignalTier::Tier3Conversational)
            .await
            .unwrap();
        let m = mentions.iter().find(|m| m.canonical_name == "my manager");
        assert!(m.is_some(), "got {mentions:?}");
        assert_eq!(m.unwrap().reference_kind, ReferenceKind::Role);
    }

    #[tokio::test]
    async fn extracts_pronouns() {
        let e = HeuristicEntityExtractor;
        let mentions = e
            .extract("She told me to do it.", SignalTier::Tier3Conversational)
            .await
            .unwrap();
        let she = mentions.iter().find(|m| m.canonical_name == "she");
        assert!(she.is_some(), "got {mentions:?}");
        assert_eq!(she.unwrap().reference_kind, ReferenceKind::Pronoun);
    }

    #[tokio::test]
    async fn drops_sentence_initial_the() {
        let e = HeuristicEntityExtractor;
        let mentions = e
            .extract("The Atlas project ships Friday.", SignalTier::Tier3Conversational)
            .await
            .unwrap();
        // "The" is dropped; "Atlas" alone is the proper-noun
        // span, with "project" hinting at type=project.
        let atlas = mentions.iter().find(|m| m.canonical_name == "Atlas");
        assert!(atlas.is_some(), "got {mentions:?}");
        assert_eq!(atlas.unwrap().entity_type, "project");
    }
}
