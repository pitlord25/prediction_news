# from fuzzywuzzy import process

# array_of_strings = ["presidential election", "election of president", "general election", "voting process", "choosing a leader"]

# search_string = "election of president"
# results = process.extract(search_string, array_of_strings, limit=3)

# print(results)  # [('election of president', 100), ('presidential election', 90), ('general election', 80)]
from fuzzywuzzy import fuzz

str1 = "President election"
# str1 = "Who will win president election in 2024?"
str2 = "US president election"

similarity = fuzz.ratio(str1, str2)
print(f"FuzzyWuzzy Similarity: {similarity}")

# from sentence_transformers import SentenceTransformer, util

# # Load a pre-trained model
# model = SentenceTransformer('paraphrase-MiniLM-L6-v2')

# # Encode sentences
# embedding1 = model.encode("Who will win president election in 2024?")
# embedding2 = model.encode("US president election")

# # Compute cosine similarity
# similarity = util.cos_sim(embedding1, embedding2)
# print("Sentence Transformers Similarity:", similarity.item())

# import Levenshtein

# str1 = "Who will win president election in 2024?"
# str2 = "President election"

# distance = Levenshtein.distance(str1, str2)
# print(f"Levenshtein Distance: {distance}")

# from difflib import SequenceMatcher

# def similar(a, b):
#     return SequenceMatcher(None, a, b).ratio()

# print(similar(str1, str2))