from mrjob.job import MRJob
from mrjob.step import MRStep
from pyparsing import makeHTMLTags
import html
import nltk
import string
import re

WORD_RE = re.compile(r"[\w']+")


def tokenizer(text):
    tokens = nltk.word_tokenize(text)
    tokens = [w.lower() for w in tokens]
    return tokens


def remove_punctuations(tokens):
    table = str.maketrans('', '', string.punctuation)
    stripped = [w.translate(table) for w in tokens]
    words = [word for word in stripped if word.isalpha()]
    return words


stemmer = nltk.stem.snowball.SnowballStemmer("porter")
word = stemmer.stem("sockets")


class MRExpertFinderWithPostCount(MRJob):

    def mapper_get_posts(self, _, line):
        rowTag, rowEndTag = makeHTMLTags("row")
        for row in rowTag.searchString(line):
            row = dict(row)
            text = re.sub('<[^<]+?>', '', row['body'])
            text = html.unescape(text)
            tokens = remove_punctuations(tokenizer(text))
            words = [stemmer.stem(w) for w in tokens]
            if word in words:
                yield row['owneruserid'], 1

    def combiner_count_posts(self, ca, counts):
        yield ca, sum(counts)

    def reducer_sum_word_counts(self, ca, counts):
        yield None, (sum(counts), ca)

    def reducer_sort_counts(self, _, ca_post_count):
        for post_count, ca in sorted(ca_post_count, reverse=True):
            yield ca, int(post_count)

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_posts,
                   combiner=self.combiner_count_posts,
                   reducer=self.reducer_sum_word_counts),
            MRStep(reducer=self.reducer_sort_counts)
        ]


if __name__ == '__main__':
    MRExpertFinderWithPostCount.run()
