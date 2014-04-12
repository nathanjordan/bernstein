from nltk.corpus import reuters
from nltk.classify import NaiveBayesClassifier
import nltk


def bag_of_words(words):
    return dict([(word, True) for word in words])

features = bag_of_words(["oil", "arab", "economy", "iraq", "iran"])

train_docs = [(bag_of_words(reuters.words(fileid)), category)
              for category in reuters.categories()
              for fileid in reuters.fileids(category) if
              fileid.startswith("train")]

#test_docs = [(bag_of_words(reuters.words(fileid)), category)
#             for category in reuters.categories()
#             for fileid in reuters.fileids(category) if
#             fileid.startswith("test")]

classifier = NaiveBayesClassifier.train(train_docs)

print classifier.classify(features)
