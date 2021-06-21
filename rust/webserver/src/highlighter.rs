use aho_corasick::{AhoCorasickBuilder, AhoCorasick};
use crate::IndexWorker;

use tracing::{instrument, debug_span, error, debug};


use serde::{Serialize, Serializer};
use serde_json::{
    Serializer as JsonSerializer,
    json,
};
use crate::IndexWorker::ResultsList;
use std::collections::HashMap;

use std::path::Path;
use std::io::Cursor;


fn highlight_matches(str: &str, aut: &AhoCorasick<u16>) -> (usize, Vec<(usize, usize)>) {
    // supports maximum 64 terms of query
    let mut processed = HashMap::<u16, u16>::new();

    let _start_index = 0;

    let mut res = Vec::new();

    for match_ in aut.find_iter(str) {
        let pat_idx = match_.pattern() as u16;

        let already_highlighted = if let Some(v) = processed.get_mut(&pat_idx) {
            *v += 1;
            *v
        } else {
            processed.insert(pat_idx, 1);
            1
        };

        if already_highlighted < 6 {
            res.push((match_.start(), match_.end()));
        }

        if processed.len() >= aut.pattern_count() && processed.iter().all(|x| *x.1 >= 5u16) {
            break;
        }
    }

    (processed.len(), res)
}


#[derive(Default, Serialize)]
pub struct HighlightResult {
    pub matches: Vec<(usize, usize)>,
    pub document: String,
}

fn load_matches_for_file<APath: AsRef<Path>>(p: APath, aut: &AhoCorasick<u16>) -> Option<HighlightResult> {
    // If we've used up more than 1.5 seconds already, exit and just show the results we already have.
    // if starttime.elapsed().unwrap().as_millis() > 50000 { break; }
    let mut docsize = 20000usize;
    let str = IndexWorker::load_file_to_string(p.as_ref(), docsize)?;

    return Some(HighlightResult {
        document: str,
        matches: vec![(0, 0)],
    });

    let (str, curmatches) = loop {
        let str = IndexWorker::load_file_to_string(p.as_ref(), docsize)?;

        // Highlighting done here, we want to measure exact time it takes to highlight.
        let (num_patterns, curmatches) = highlight_matches(&str, &aut);

        if num_patterns >= aut.pattern_count() {
            break (str, curmatches);
        }
        if str.len() < docsize {
            if num_patterns == 0 {
                debug!(path = &*p.as_ref().to_string_lossy(), num_patterns, matches = ?curmatches, "Highlighting error: unable to find any matches");
            }
            break (str, curmatches);
        }
        docsize *= 2;
    };
    Some(HighlightResult {
        document: str,
        matches: curmatches,
    })
}


pub fn serialize_highlight_response(mut t: Vec<(String, HighlightResult)>) -> String {
    // let est_size = {
    //     t.iter().fold(0, |sum, elem| sum + elem.1.document.len() + elem.0.len()) as f32 * 1.2f32
    // } as usize;

    let t: Vec<_> = t.drain(..).map(|elem| {
        json!({
            "filename": elem.0,
            "document": elem.1.document,
            "matches": elem.1.matches
        })
    }).collect();


    let mut out = Vec::with_capacity(10000);
    let cursorout = Cursor::new(&mut out);
    let mut serializer = JsonSerializer::new(cursorout);

    serializer.collect_seq(t).unwrap();
    String::from_utf8(out).unwrap()
}

#[instrument(level = "debug", skip(filelist))]
pub fn highlight_files(filelist: &ResultsList, highlight_words: &[String]) -> Vec<(String, HighlightResult)> {
    assert!(highlight_words.len() < 64);


    let _starttime = std::time::SystemTime::now();
    let mut highlights = Vec::new();

    let aut = AhoCorasickBuilder::new().ascii_case_insensitive(true)
        .dfa(true)
        .build_with_size::<u16, _, _>(highlight_words)
        .expect("Number of terms too large for a `u8` DFA to support");

    let _highlight_span = debug_span!("Highlight matches");
    for (_, path) in filelist.iter().rev() {
        let hr = load_matches_for_file(path, &aut);

        if hr.is_none() { continue; };

        let hr = hr.unwrap();

        if !hr.matches.is_empty() {
            highlights.push((path.clone(), hr));
        } else {
            error!(%path, "Couldn't find any matches");
        }

        if highlights.len() >= 10 { break; }
    }

    highlights
}