from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")

class MRInvertedIndex(MRJob):

    def mapper(self, _, line):
        document_id, text = line.split(": ", 1)
        words = WORD_RE.findall(text)
        for word in words:
            yield word.lower(), document_id

    def reducer(self, word, document_ids):
        document_ids_list = list(set(document_ids))  # Remove duplicate document_ids
        document_ids_list.sort()  # Sort document_ids for consistency
        yield word, ", ".join(document_ids_list)

if __name__ == '__main__':
    MRInvertedIndex.run()
