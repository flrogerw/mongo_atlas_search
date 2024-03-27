import textstat # https://pypi.org/project/textstat/
import re


class Grader:

    @staticmethod
    def get_readability(text_str):
        try:
            text_str = ' '.join(
                re.sub("((\Swww.[^\s]+)|(\S*@\S*\s?)|#[A-Za-z0-9]+)|(@[A-Za-z0-9]+)|(https?:[^\s]+)",
                       " ", text_str, flags=re.MULTILINE).split())
            return textstat.text_standard(text_str, float_output=True)
        except Exception:
            raise
