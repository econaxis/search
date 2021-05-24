use std::path::Path;
use aho_corasick::AhoCorasickBuilder;
use crate::IndexWorker;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tracing::instrument;
use std::sync::Mutex;


fn highlight_matches(str: &str, terms: &[String]) -> Vec<(usize, usize)> {
    let aut = AhoCorasickBuilder::new().ascii_case_insensitive(true).build(terms);

    // supports maximum 32 terms of query
    let mut processed = [0u8; 32];

    aut.find_iter(str).filter_map(|match_| {
        if processed[match_.pattern()] < 4 {
            processed[match_.pattern()]+=1;
            Some((match_.start(), match_.end()))
        } else {
            None
        }
    }).collect()
}

pub async fn highlight_files<T: AsRef<str>, AWrite: AsyncWrite + std::marker::Unpin>(
    filelist: &[T], highlight_words: &[String], mut writer: &mut AWrite) {
    for path in filelist {
        let path = path.as_ref();
        let str = match IndexWorker::load_file_to_string(path.as_ref()) {
            None => "".to_owned(),
            Some(x) => x
        };

        let mut str = str.as_str();
        // Limit highlighting to first 5kb only
        let mut strindices: Vec<usize> = str.char_indices().map(|(pos, _)| pos).collect();
        if str.len() > 50000 {
            str = &str[0..strindices[50000]];
            strindices.truncate(50000);
        }


        let mut matches = highlight_matches(str, highlight_words);

        writer.write_all(format!("File {}\n", path).as_bytes()).await;

        // Declare 100kb buffer on stack to hold all data.
        let mut simplebuffer = [0u8; 10000];
        let mut bufferstackpointer = 0usize;

        for (start, end) in matches {
            // Start a new chunk.
            let lastend = (end + 5).clamp(0, str.len() - 1);
            let firstbegin = if start > 5 { start - 5 } else { 0 };

            let lastend = strindices[strindices.partition_point(|&x| x <= lastend) - 1];
            let firstbegin = strindices[strindices.partition_point(|&x| x <= firstbegin) - 1];

            let s = format!("{}<mark>{}</mark>{} || ", &str[firstbegin..start], &str[start..end], &str[end..lastend]);
            let s = s.as_bytes();
            let slen = s.len();

            if bufferstackpointer + slen < simplebuffer.len() {
                simplebuffer[bufferstackpointer..bufferstackpointer + slen].copy_from_slice(s);
                bufferstackpointer += slen;
            } else {
                writer.write_all(&simplebuffer[0..bufferstackpointer]).await;
                simplebuffer[..slen].copy_from_slice(s);
                bufferstackpointer = slen;
            }
        }
        writer.write_all(&simplebuffer[0..bufferstackpointer]).await;
        writer.write_all(b"\n\n\n").await;
    }
}