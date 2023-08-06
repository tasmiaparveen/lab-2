from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")

class MRWordBigramCount(MRJob):

    def mapper(self, _, line):
        words = WORD_RE.findall(line)
        for i in range(len(words) - 1):
            bigram = f"{words[i].lower()},{words[i + 1].lower()}"
            yield bigram, 1

    def reducer(self, bigram, counts):
        yield bigram, sum(counts)

if __name__ == '__main__':
    MRWordBigramCount.run()
