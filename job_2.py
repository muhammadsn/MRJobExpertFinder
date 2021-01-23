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


def remove_stopwords(sw, tokens):
    all_discriminative_words = [w for w in tokens if w not in set(sw)]
    return all_discriminative_words


stemmer = nltk.stem.snowball.SnowballStemmer("porter")
stop_words = nltk.corpus.stopwords.words('english')
html_tags = ["a", "abbr", "acronym", "address", "area", "b", "base", "bdo", "big", "blockquote", "body", "br", "button", "caption", "cite",
             "code", "col", "colgroup", "dd", "del", "dfn", "div", "dl", "DOCTYPE", "dt", "em", "fieldset", "form", "h1", "h2", "small",
             "h3", "h4", "h5", "h6", "head", "html", "hr", "i", "img", "input", "ins", "kbd", "label", "legend", "li", "link", "select",
             "map", "meta", "noscript", "object", "ol", "optgroup", "option", "p", "param", "pre", "q", "samp", "script", "tt", "ul",
             "span", "strong", "style", "sub", "sup", "table", "tbody", "td", "textarea", "tfoot", "th", "thead", "title", "tr", "var"]
all_stop_words = stop_words + html_tags

word = stemmer.stem("sockets")

class MRMostUsedWords(MRJob):

    def mapper_get_posts(self, _, line):
        rowTag, rowEndTag = makeHTMLTags("row")
        for row in rowTag.searchString(line):
            row = dict(row)
            text = re.sub('<[^<]+?>', '', row['body'])
            text = html.unescape(text)
            tokens = remove_stopwords(all_stop_words, remove_punctuations(tokenizer(text)))
            words = [stemmer.stem(w) for w in tokens]
            if word in words:
                for w in words:
                    yield w, 1

    def combiner_count_posts(self, w, counts):
        yield w, sum(counts)

    def reducer_sum_word_counts(self, w, counts):
        yield None, (sum(counts), w)

    def reducer_sort_counts(self, _, w_count):
        for count, w in sorted(w_count, reverse=True):
            yield w, int(count)

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_posts,
                   combiner=self.combiner_count_posts,
                   reducer=self.reducer_sum_word_counts),
            MRStep(reducer=self.reducer_sort_counts)
        ]


if __name__ == '__main__':
    MRMostUsedWords.run()
