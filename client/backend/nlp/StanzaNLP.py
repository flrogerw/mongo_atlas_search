import json
import torch
import stanza
from stanza.pipeline.multilingual import Pipeline
from stanza.pipeline.core import DownloadMethod
from bs4 import BeautifulSoup
import emoji

words = open('nlp/bad_word_list.json')
bad_words_list = json.load(words)
stanza.download(lang="multilingual")


class StanzaNLP(Pipeline):
    def __init__(self, languages, *args, **kwargs):
        self.text_processors = {}
        for language in languages:
            self.text_processors[language] = Pipeline(download_method=DownloadMethod.REUSE_RESOURCES, lang=language,
                                                      processors='tokenize,pos,lemma')

        super().__init__(download_method=DownloadMethod.REUSE_RESOURCES,
                         lang="multilingual", processors="langid", langid_clean_text=True)

    @staticmethod
    def get_language(nlp):
        try:
            return nlp.lang
        except Exception:
            raise

    @staticmethod
    def remove_stopwords(sents):
        try:
            sentences = []
            for sent in sents:
                for word in sent.words:
                    if word.upos not in ['PUNCT', 'PRON', 'ADP', 'CCONJ', 'DET', 'PART', 'SYM']:
                        sentences.append(word)
            return sentences
        except Exception:
            raise

    @staticmethod
    def get_words(nlp):
        sentences = []
        for sent in nlp.sentences:
            for word in sent.words:
                if word.upos not in ['PUNCT', 'PART', 'SYM']:
                    sentences.append(word)
        return sentences

    def get_lemma(self, text_str, language):
        try:
            nlp = self.text_processors[language](text_str)
            no_stopwords = self.remove_stopwords(nlp.sentences)
            return ' '.join([word.lemma.lower() for word in no_stopwords])
        except Exception:
            raise

    @staticmethod
    def get_vector(text, model):
        try:
            with torch.no_grad():
                vector = model.encode(text)
            return vector.tolist()
        except Exception:
            raise

    @staticmethod
    def profanity_check(text, fields_to_check, profanity):
        bad_words = bad_words_list[text['language']]
        profanity_check_str = ' '.join(list(map(lambda a, r=text: r[a], fields_to_check)))
        profanity.load_censor_words(bad_words)
        return profanity.contains_profanity(profanity_check_str)



    @staticmethod
    def clean_text(text):
        try:
            soup = BeautifulSoup(text, "html5lib")
            [data.decompose() for data in soup(['style', 'script'])]
            clean_text = emoji.replace_emoji(' '.join(soup.stripped_strings), replace='')
            clean_text = " ".join(clean_text.split())
            return clean_text
        except Exception:
            raise
