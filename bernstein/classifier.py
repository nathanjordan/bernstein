from nltk.corpus import reuters
from nltk.classify import NaiveBayesClassifier
import logging
import database

logging.basicConfig(level=logging.WARNING)


class DocumentClassifier(object):
    """ Classifies documents based on the reuters corpus """

    def __init__(self):
        # Generate training set from sample of Reuters corpus
        train_docs = [(self.bag_of_words(reuters.words(fileid)), category)
                      for category in reuters.categories()
                      for fileid in reuters.fileids(category) if
                      fileid.startswith("train")]
        # Create a classifier from the training data
        self.classifier = NaiveBayesClassifier.train(train_docs)

    def bag_of_words(self, words):
        return dict([(word, True) for word in words])

    def classify_document_contents(self, word_list):
        # Turn the list of words into a feature list
        features = self.bag_of_words(word_list)
        # Classify the word list
        return self.classifier.classify(features)

    # def test_classifier(self):
    #     test_docs = [(bag_of_words(reuters.words(fileid)), category)
    #                  for category in reuters.categories()
    #                  for fileid in reuters.fileids(category) if
    #                  fileid.startswith("test")]


class GraphClassifier(object):

    def __init__(self):
        # print reuters categories
        print "reuters categories"
        print reuters.categories()
        # TODO this is probably bad
        print "getting nodes"
        self.nodes = database.get_all_nodes()
        print "training classifier"
        self.classifier = DocumentClassifier()

    def classify_nodes(self):
        print "classifying nodes"
        for node in self.nodes:
            word_list = node['word_list']
            if len(word_list) < 500:
                node['classifiers'] = ["NED"]
            else:
                classifiers = \
                    self.classifier.classify_document_contents(word_list)
                node['classifiers'] = classifiers

if __name__ == "__main__":
    gc = GraphClassifier()
    gc.classify_nodes()
