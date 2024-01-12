import re
import torch
from unidecode import unidecode
import nltk
from nltk.corpus import wordnet
from nltk import WordNetLemmatizer
from nltk.tokenize import word_tokenize
from bs4 import BeautifulSoup
import simplemma
from simplemma import simple_tokenizer

STOPWORDS = set(nltk.corpus.stopwords.words(['english', 'spanish']))
CLEANER = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')

lem = WordNetLemmatizer()
filters = ['!', '"', '#', '$', '%', '&', '(', ')', '*', '+', '-', '.', '/', '\\', ':', ';', '<', '=', '>',
           '?', '@', '[', ']', '^', '_', '`', '{', '|', '}', '\t', "'", ",", '~', '—']


class ProcessText:
    def __init__(self, text, model=None, lang='en'):
        self.language = lang
        self.model = model
        self.clean = None
        self.vector = None
        self.tokens = None
        self.raw = self.clean_text(text)
        self.strip_text()
        self.tokenize()

    def get_tokens(self):
        return self.tokens

    @staticmethod
    def clean_text(text):
        try:
            return BeautifulSoup(text, "html5lib").get_text()
        except Exception:
            raise

    def get_clean(self):
        return self.clean

    @staticmethod
    def pos_tagger(nltk_tag):
        try:
            if nltk_tag.startswith('J'):
                return wordnet.ADJ
            elif nltk_tag.startswith('V'):
                return wordnet.VERB
            elif nltk_tag.startswith('N'):
                return wordnet.NOUN
            elif nltk_tag.startswith('R'):
                return wordnet.ADV
            else:
                return None
        except Exception:
            raise

    def multilingual_lemma(self, phrase):
        try:
            tokens = simple_tokenizer(phrase)
            self.tokens = " ".join([simplemma.lemmatize(t, lang=self.language) for t in tokens])
        except Exception:
            raise

    def lemmatizer(self, phrase):
        try:
            pos_tagged = nltk.pos_tag(nltk.word_tokenize(phrase))
            wordnet_tagged = list(map(lambda x: (x[0], self.pos_tagger(x[1])), pos_tagged))
            lemmatized_sentence = []
            for word, tag in wordnet_tagged:
                if tag is None:
                    lemmatized_sentence.append(word)
                else:
                    lemmatized_sentence.append(lem.lemmatize(word, tag))
            self.tokens = " ".join(lemmatized_sentence)
        except Exception:
            raise

    def get_vector(self):
        try:
            if self.vector is None:
                with torch.no_grad():
                    self.vector = self.model.encode(self.clean)
            return self.vector
        except Exception:
            raise

    def strip_text(self):
        try:
            self.clean = re.sub(r"([\r+,\n+,\t+])", ' ', re.sub(CLEANER, '', unidecode(self.raw)
                                                                .replace('\"', "'").replace('|', ' '))).replace('  ',
                                                                                                                ' ')
        except Exception:
            raise

    def tokenize(self, stopwords=STOPWORDS):
        try:
            translation_table = {ord(char): ord(' ') for char in filters}
            s = self.clean.translate(translation_table)
            s = s.lower()
            s = " ".join(s.split())
            s = s.strip(" ")
            self.multilingual_lemma(s)
            # word_tokens = word_tokenize(s)
        # s = [w for w in word_tokens if not w.lower() in stopwords]
        # s = " ".join(s)
        # self.lemmatizer(s)
        except Exception:
            raise
