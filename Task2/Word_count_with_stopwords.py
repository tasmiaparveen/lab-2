from mrjob.job import MRJob
import re

STOPWORDS = set(['the', 'and', 'of', 'a', 'to', 'in', 'is', 'it'])
WORD_RE = re.compile(r"[\w']+")

class MRWordCountWithoutStopwords(MRJob):

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            word = word.lower()
            if word not in STOPWORDS:
                yield word, 1

    def reducer(self, word, counts):
        yield word, sum(counts)

if __name__ == '__main__':
    MRWordCountWithoutStopwords.run()
