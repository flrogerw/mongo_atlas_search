import re
import torch
from unidecode import unidecode
import nltk
from nltk import WordNetLemmatizer
from bs4 import BeautifulSoup
import simplemma
from simplemma import simple_tokenizer
nltk.download('stopwords')

STOPWORDS = set(nltk.corpus.stopwords.words(['english', 'spanish']))
CLEANER = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')

lem = WordNetLemmatizer()
filters = ['!', '"', '#', '$', '%', '&', '(', ')', '*', '+', '-', '.', '/', '\\', ':', ';', '<', '=', '>',
           '?', '@', '[', ']', '^', '_', '`', '{', '|', '}', '\t', "'", ",", '~', '—']


class ProcessText:
    def __init__(self, text, lang='en'):
        self.language = lang
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

    def multilingual_lemma(self, phrase):
        try:
            tokens = simple_tokenizer(phrase)
            self.tokens = " ".join([simplemma.lemmatize(t, lang=self.language) for t in tokens])
        except Exception:
            raise

    @staticmethod
    def get_vector(text, model):
        try:
            with torch.no_grad():
                vector = model.encode(text)
            return vector
        except Exception:
            raise

    def strip_text(self):
        try:
            self.clean = re.sub(r"([\r+,\n+,\t+])", ' ', re.sub(CLEANER, '', unidecode(self.raw)
                                                                .replace('\\"', "'")
                                                                .replace('|', ' '))).replace('  ', ' ')
        except Exception:
            raise

    def tokenize(self):
        try:
            translation_table = {ord(char): ord(' ') for char in filters}
            s = self.clean.translate(translation_table)
            s = s.lower()
            s = " ".join(s.split())
            s = s.strip(" ")
            self.multilingual_lemma(s)
        except Exception:
            raise
