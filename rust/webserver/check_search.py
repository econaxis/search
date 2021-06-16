
import requests

url = "http://henry-80q7:8080/search?q={}"
highlighturl = "http://henry-80q7:8080/highlight?id={}"

words = requests.get("https://raw.githubusercontent.com/first20hours/google-10000-english/master/google-10000-english-usa-no-swears.txt").text
words = words.splitlines()

highlightid = 0

for i in range(len(words)):
    if len(words[i]) < 5 or len(words[i+1]) < 5:
        continue
    print(len(requests.get(url.format(words[i] + "+" + words[i+1])).text))

    print(len(requests.get(highlighturl.format(highlightid)).text), " hi")
    highlightid += 1