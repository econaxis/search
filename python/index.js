function clear_url_list() {
    document.getElementById('urls').innerHTML = ""
}

function add_url_list(url, highlights) {
    let node = document.getElementById("preview-template").content.children[0].cloneNode(true);
    node.querySelector("div h4 a").href = url;
    node.querySelector("div h4 a").innerText = url;
    node.querySelector("div p").innerText = highlights;
    return node;
}

function insert(text, index, base) {
    return base.substring(0, index) + text + base.substring(index);
}

function update_match_index(index) {
    document.getElementById("current").innerText = index;
    document.getElementById("total").innerText = window.bookmarks.length;
}

function next_match(type) {
    let padding = 20;
    let current_offset = document.getElementById("website-display").scrollTop;
    for(let index = 0; index < window.bookmarks.length; index++) {
        const j = window.bookmarks[index];
        if (j > current_offset + padding || index == window.bookmarks.length - 1) {
            if (type === "prev") {
                j = window.bookmarks[index - 2];
                index -= 2;
            }
            if (type !== "update") {
                document.getElementById("website-display").scrollTo({top: j - padding, behaviour: 'smooth'});
            }
            console.log("Scrolling to", j);
            update_match_index(index + 1)
            break;
        }
    };
}

function query_onchange(event) {
    console.log("query ", event.target.value);
    query(...event.target.value.split(' '));
}

function prev_match() {
    let current_offset = document.getElementById("website-display").scrollTop;
    for (j of window.bookmarks.slice().reverse()) {
        if (j < current_offset + 50) {
            document.getElementById("website-display").scrollTo({top: j - 50, behaviour: 'smooth'});
            break;
        }
    }
}

function to_next_word(text, index, direction) {
    let ch = text[index];
    while (ch !== undefined && ch.match("[a-zA-Z]")) {
        index += direction;
        ch = text[index];
    }
    return index;
}

function set_darken(node) {
    for (const n of document.querySelectorAll(".darken")) {
        n.classList.remove("darken");
    }
    node.classList.add("darken");
}

function click_handler(node, matches) {
    return (text) => {
        set_darken(node);
        document.getElementById("website-display").innerHTML = "";
        match = matches.sort((a, b) => a[0] - b[0]);
        console.log(match);
        let left_index = 0;

        let bookmarks = [];
        for (let [pos, length] of match) {
            let pos_expanded = to_next_word(text, Math.max(pos - 10, 0), -1);
            let end = pos + length + 20;
            end = to_next_word(text, end, 1);
            console.assert(pos_expanded < text.length && left_index < text.length);
            let left_node = document.createElement("span");
            left_node.innerText = text.slice(left_index, pos_expanded);

            let middle_node = document.createElement("mark");
            middle_node.innerText = text.slice(pos_expanded, end);

            left_index = end;
            left_node = document.getElementById("website-display").appendChild(left_node);
            document.getElementById("website-display").appendChild(middle_node);

            bookmarks.push(middle_node.offsetTop - document.getElementById("website-display").offsetTop);
        }
        window.bookmarks = bookmarks;
        let remaining_node = document.createElement("span");
        remaining_node.innerText = text.slice(left_index);
        document.getElementById("website-display").appendChild(remaining_node);

        update_match_index(0);
    }
}

async function query() {
    document.title = "Working...";
    arguments = Array.prototype.slice.call(arguments, 0);
    arguments = arguments.join('+');
    console.log(arguments)
    let f = await fetch(`http://localhost:5000/${arguments}`);
    let response = await f.json();

    let json = response.text;
    let matches = response.matches;
    let scores = response.scores;
    console.log(json, matches);

    clear_url_list();
    for (const id of scores) {
        if (!json.hasOwnProperty(id)) {
            continue;
        }

        let url = json[id][0];
        let contents = json[id][1];
        let node = add_url_list(url, contents);
        node.addEventListener('click', () => {
            fetch(`http://localhost:5000/id/${id}`).then(resp => resp.text()).then(click_handler(node, matches[id]))
        })
        document.getElementById("urls").appendChild(node);
    }
    document.title = "Search Engine";
}

if (document.getElementById("query-input").value != "") {
    query(...document.getElementById("query-input").value.split(' '));
} else {
    query("alberts", "world")

}