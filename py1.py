filename = "all_cases.txt"


with open(filename, "r") as f:
    text = f.read()
    words = text.split()
    word_count = len(words)

print("Number of words:", word_count)
