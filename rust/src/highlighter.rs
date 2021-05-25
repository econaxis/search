use std::path::Path;
use aho_corasick::AhoCorasickBuilder;
use crate::IndexWorker;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tracing::{instrument, debug};
use std::sync::Mutex;
use std::collections::HashMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::ser::SerializeMap;
use std::time::Duration;


fn highlight_matches(str: &str, terms: &[String]) -> Vec<(usize, usize)> {
    let aut = AhoCorasickBuilder::new().ascii_case_insensitive(true).build(terms);

    // supports maximum 32 terms of query
    let mut processed = [0u8; 32];

    aut.find_iter(str).filter_map(|match_| {
        if processed[match_.pattern()] < 5 {
            processed[match_.pattern()] += 1;
            Some((match_.start(), match_.end()))
        } else {
            None
        }
    }).collect()
}

const FIRST_N_BYTES_ONLY: usize = 300000;

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
pub fn highlight_files<T: AsRef<str>>(filelist: &[T], highlight_words: &[String]) -> Vec<(String, Vec<String>)> {
    let starttime = std::time::SystemTime::now();
    let mut highlights = Vec::new();
    for path in filelist {
        if starttime.elapsed().unwrap().as_millis() > 1500 {
            let dum = false;
            break;
        }

        let path = path.as_ref();
        let str = match IndexWorker::load_file_to_string(path.as_ref()) {
            None => {
                debug!(path, "File doesn't exist");
                "".to_owned()
            },
            Some(x) => x
        };
        let mut str = str.as_str();

        // Limit highlighting to first 5kb only
        let mut strindices: Vec<usize> = str.char_indices().map(|(pos, _)| pos).collect();
        if str.len() > FIRST_N_BYTES_ONLY {
            str = &str[0..strindices[FIRST_N_BYTES_ONLY]];
            strindices.truncate(FIRST_N_BYTES_ONLY);
        }


        let mut matches = highlight_matches(str, highlight_words);



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
        }

        // 20 highlighted files is enough for the first page. We don't need to highlight all.
        if highlights.len() > 10 {
            break;
        }
    }

    highlights
}