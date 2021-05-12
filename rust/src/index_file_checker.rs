use fancy_regex::{Regex, RegexBuilder, Match, Captures};
use std::fs;
use std::path::Path;
use std::io::{Read, Write};
use std::fs::File;
use std::time::SystemTime;

const space: char = ' ';
const PATH: &str = "/mnt/nfs/.cache/data-files/";

enum FileTypes {
    positions,
    frequencies,
    terms,
}
fn get_filename(t: FileTypes, suffix: &str) -> String {
    match t {
        FileTypes::positions => format!("{}-{}", "positions", suffix),
        FileTypes::terms => format!("{}-{}", "terms", suffix),
        FileTypes::frequencies => format!("{}-{}", "frequencies", suffix)
    }
}

fn check() {
    let data_path = Path::new(PATH);
    let indice_path = data_path.join("indices");
    let index_file_path = data_path.join("indices/index_files");
    let mut index_file = fs::File::open(&index_file_path).unwrap();
    let time = std::time::SystemTime::now();


    let mut contents = String::new();

    index_file.read_to_string(&mut contents);

    fs::DirBuilder::new().create(indice_path.join("processed"));

    let re = RegexBuilder::new(r"# joined (?!replaced)(.*)").build().unwrap();
    let mut captures_list: Vec<(usize, usize, String)> = re.captures_iter(contents.as_str()).map(|x| {
        let x = x.unwrap();
        let x = x.get(1).unwrap();
        (x.start(), x.end(), x.as_str().to_string())
    }).collect();
    for (start, end, ref filename) in &captures_list {
        let rename = |filetype: FileTypes| {
            let freq = get_filename(filetype, filename);
            if !indice_path.join(&freq).exists() {
                // File doesn't exist!
                eprintln!("File \"{}\" doesn't exist", freq);
            } else {
                fs::rename(indice_path.join(&freq), indice_path.join("processed").join(&freq));
            }
        };
        rename(FileTypes::frequencies);
        rename(FileTypes::terms);
        rename(FileTypes::positions);
    }

    let mut newstr = String::new();
    if captures_list.is_empty() { newstr = contents.clone(); } else {
        captures_list.iter().fold(0, |prev_end, (start, end, filename)| {
            newstr.push_str(&contents[prev_end..*start]);
            newstr.push_str(format!("replaced \"{}\"", filename).as_str());
            return *end;
        });
        captures_list.last().map(|(_start, last_end, ..)| {
            newstr.push_str(&contents[*last_end..]);
        });
    }
    fs::rename(&index_file_path, indice_path.join(
        format!("index_files.{}.bak", time.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs())));

    fs::File::create(&index_file_path).unwrap().write(newstr.as_bytes());
}