use std::path::Path;
use aho_corasick::AhoCorasickBuilder;
use crate::IndexWorker;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tracing::{instrument, debug, debug_span};
use std::sync::Mutex;
use std::collections::HashMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::ser::SerializeMap;
use std::time::Duration;
use crate::IndexWorker::ResultsList;
use std::ops::Deref;

fn highlight_matches(str: &str, terms: &[String]) -> Vec<(usize, usize)> {
    assert!(terms.len() < 64);

    let aut = AhoCorasickBuilder::new().ascii_case_insensitive(true).build(terms);

    // supports maximum 64 terms of query
    let mut processed = [0u8; 64];
    let processed = &mut processed[0..terms.len()];

    let start_index = 0;

    let mut res = Vec::new();

    for match_ in aut.find_iter(str) {
        if processed[match_.pattern()] < 5 {
            processed[match_.pattern()] += 1;
            res.push((match_.start(), match_.end()));
        }

        // We have found all the matches we need to, so exit.
        if processed.iter().all(|&x| x >= 5u8) {
            break;
        }
    }

    return res;
}


struct CustomDeserialize<'a>(&'a Vec<(String, Vec<String>)>);

impl Serialize for CustomDeserialize<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error> where
        S: Serializer {
        let mut map = serializer.serialize_map(Some(self.0.len())).unwrap();

        for (name, highlights) in self.0.iter() {
            map.serialize_entry(name, highlights);
        };
        map.end()
    }
}

pub fn serialize_response_to_json(a: &Vec<(String, Vec<String>)>) -> String {
    let a = CustomDeserialize(a);
    serde_json::to_string(&a).unwrap()
}

#[instrument(level = "debug", skip(filelist))]
pub fn highlight_files(filelist: &ResultsList, highlight_words: &[String]) -> Vec<(String, Vec<String>)> {
    let starttime = std::time::SystemTime::now();
    let mut highlights = Vec::new();

    let _highlight_span = debug_span!("Highlight matches");
    for (_, path) in filelist.iter().rev() {

        // If we've used up more than 1.5 seconds already, exit and just show the results we already have.
        // if starttime.elapsed().unwrap().as_millis() > 50000 { break; }

        let _sp = debug_span!("Loading file", file = %path).entered();
        let str = IndexWorker::load_file_to_string(path.as_ref());

        if str.is_none() {
            continue;
        }
        let str = str.unwrap();
        _sp.exit();
        let mut str = str.as_str();

        // Highlighting done here, we want to measure exact time it takes to highlight.
        let _sp = _highlight_span.enter();
        let mut matches = highlight_matches(str, highlight_words);
        std::mem::drop(_sp);


        let mut highlight_hits = Vec::new();
        for (start, end) in matches {
            let beforestart: String = str[0..start].chars().rev().take(20).collect();
            let beforestart: String = beforestart.chars().rev().collect();
            let afterend: String = str[end..].chars().take(20).collect();
            let real_highlight: &str = &str[start..end];
            let s = format!("{}((({}))){}", beforestart, real_highlight, afterend);
            highlight_hits.push(s);
        }

        if !highlight_hits.is_empty() {
            highlights.push((path.to_owned(), highlight_hits));
        } else {
            debug!(%path, "Couldn't find any matches");
        }

        // 10 highlighted files is enough for the first page. We don't need to highlight all.
        if highlights.len() >= 10 {
            break;
        }
    }

    highlights
}