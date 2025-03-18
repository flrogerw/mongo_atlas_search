import json
import torch
import stanza
from stanza.pipeline.multilingual import Pipeline
from stanza.pipeline.core import DownloadMethod
from bs4 import BeautifulSoup
import emoji
from typing import List, Dict, Any

# Load the list of bad words from a JSON file
with open('nlp/bad_word_list.json') as words:
    bad_words_list: Dict[str, List[str]] = json.load(words)

# Download the Stanza multilingual model
stanza.download(lang="multilingual")

class StanzaNLP(Pipeline):
    """
    A natural language processing (NLP) pipeline using Stanza for text processing,
    language identification, lemmatization, and profanity checking.
    """
    
    def __init__(self, languages: List[str], *args, **kwargs) -> None:
        """
        Initializes the StanzaNLP pipeline with specified languages.
        
        Args:
            languages (List[str]): List of languages to be supported.
        """
        self.text_processors: Dict[str, Pipeline] = {}
        for language in languages:
            self.text_processors[language] = Pipeline(
                download_method=DownloadMethod.REUSE_RESOURCES,
                lang=language,
                processors='tokenize,pos,lemma'
            )
        
        super().__init__(
            download_method=DownloadMethod.REUSE_RESOURCES,
            lang="multilingual",
            processors="langid",
            langid_clean_text=True
        )

    @staticmethod
    def get_language(nlp: Pipeline) -> str:
        """
        Extracts the detected language from a Stanza NLP object.
        
        Args:
            nlp (Pipeline): The Stanza NLP pipeline object.
        
        Returns:
            str: Detected language.
        """
        try:
            return nlp.lang
        except Exception:
            raise

    @staticmethod
    def remove_stopwords(sents: List[Any]) -> List[Any]:
        """
        Removes stopwords from tokenized sentences.
        
        Args:
            sents (List[Any]): List of Stanza tokenized sentences.
        
        Returns:
            List[Any]: List of words excluding stopwords.
        """
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
    def get_words(nlp: Pipeline) -> List[Any]:
        """
        Extracts words from tokenized sentences, excluding punctuation and symbols.
        
        Args:
            nlp (Pipeline): The Stanza NLP pipeline object.
        
        Returns:
            List[Any]: List of extracted words.
        """
        sentences = []
        for sent in nlp.sentences:
            for word in sent.words:
                if word.upos not in ['PUNCT', 'PART', 'SYM']:
                    sentences.append(word)
        return sentences

    def get_lemma(self, text_str: str, language: str) -> str:
        """
        Converts text into its lemmatized form.
        
        Args:
            text_str (str): Input text.
            language (str): Language of the text.
        
        Returns:
            str: Lemmatized text.
        """
        try:
            nlp = self.text_processors[language](text_str)
            no_stopwords = self.remove_stopwords(nlp.sentences)
            return ' '.join([word.lemma.lower() for word in no_stopwords])
        except Exception:
            raise

    @staticmethod
    def get_vector(text: str, model: Any) -> Any:
        """
        Generates an embedding vector representation for the given text using a model.
        
        Args:
            text (str): Input text.
            model (Any): Embedding model.
        
        Returns:
            Any: Text embedding vector.
        """
        try:
            with torch.no_grad():
                vector = model.encode(text)
            return vector
        except Exception:
            raise

    @staticmethod
    def profanity_check(text: Dict[str, str], fields_to_check: List[str], profanity: Any) -> bool:
        """
        Checks for profanity in the specified fields of a text dictionary.
        
        Args:
            text (Dict[str, str]): Dictionary containing text fields.
            fields_to_check (List[str]): Fields to check for profanity.
            profanity (Any): Profanity detection tool.
        
        Returns:
            bool: True if profanity is found, otherwise False.
        """
        bad_words = bad_words_list.get(text['language'], [])
        profanity_check_str = ' '.join(list(map(lambda a, r=text: r[a], fields_to_check)))
        profanity.load_censor_words(bad_words)
        return profanity.contains_profanity(profanity_check_str)

    @staticmethod
    def clean_text(text: str) -> str:
        """
        Cleans input text by removing HTML elements and emojis.
        
        Args:
            text (str): Input text.
        
        Returns:
            str: Cleaned text.
        """
        try:
            soup = BeautifulSoup(text, "html5lib")
            # Remove style and script tags
            [data.decompose() for data in soup(['style', 'script'])]
            clean_text = emoji.replace_emoji(' '.join(soup.stripped_strings), replace='')
            clean_text = " ".join(clean_text.split())
            return clean_text
        except Exception:
            raise
