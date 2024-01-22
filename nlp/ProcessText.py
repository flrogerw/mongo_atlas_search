import re
import os
import torch
import json
from unidecode import unidecode
from Errors import ValidationError
import nltk
from nltk.corpus import wordnet
from nltk import WordNetLemmatizer
# from nltk.tokenize import word_tokenize
from nlp.Grader import Grader
from bs4 import BeautifulSoup
import simplemma
from simplemma import simple_tokenizer

STOPWORDS = set(nltk.corpus.stopwords.words(['english', 'spanish']))
CLEANER = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')

print(os.getcwd())
words = open('nlp/bad_word_list.json')
bad_words_list = json.load(words)
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
    def return_clean_text(text):
        try:
            clean_text = BeautifulSoup(text, "html5lib").get_text()
            clean_text = re.sub(r"([\r+,\n+,\t+])", ' ', re.sub(CLEANER, '', unidecode(clean_text)
                                                                .replace('\"', "\'")
                                                                .replace("'", "")
                                                                .replace('|', ' '))) \
                .replace('  ', ' ')
            return clean_text
        except Exception:
            raise

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
                                                                .replace('\"', "\'")
                                                                .replace("'", "")
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

    @staticmethod
    def profanity_check(response, fields_to_check, profanity):
        bad_words = bad_words_list[response['language']]
        profanity_check_str = ' '.join(list(map(lambda a, r=response: r[a], fields_to_check)))
        profanity.load_censor_words(bad_words)
        return profanity.contains_profanity(profanity_check_str)

    @staticmethod
    def get_readability(text, nlp, grader_type='dale_chall'):
        grader = Grader(text, nlp)
        if grader_type == 'dale_chall':
            return grader.dale_chall_readability_score()
        elif grader_type == 'flesch_kincaid':
            return grader.flesch_kincaid_readability_test()
        elif grader_type == 'gunning_fog':
            return grader.gunning_fog()
        elif grader_type == 'smog_index':
            return grader.smog_index()
        else:
            return 0

    @staticmethod
    def get_language_from_model(text, nlp):
        doc = nlp(text)
        dl = doc._.language
        return dl["language"], dl["score"]

    @staticmethod
    def get_language(root, nlp, min_tolerance, languages):
        # Filter out unsupported languages
        language = root.find(".//language")
        if hasattr(language, 'text'):
            language = language.text.lower().split('-')[0]
        else:
            language_text = root.find(".//description")
            if hasattr(language_text, 'text'):
                clean_text = ProcessText.return_clean_text(language_text.text)
                get_lang = ProcessText.get_language_from_model(clean_text, nlp)
                if get_lang is None:
                    raise ValidationError("Language not supported: {}.".format(language))
                language, tolerance = get_lang
                if tolerance < float(min_tolerance):
                    raise ValidationError("Minimum Language Tolerance not met {}:{}.".format(tolerance, min_tolerance))
            else:
                raise ValidationError("Can Not Determine Language.")
        if language not in languages:
            raise ValidationError("Language not supported: {}.".format(language))
        else:
            return language
