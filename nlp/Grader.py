import textstat  # https://pypi.org/project/textstat/
import re


class Grader:
    """
    A utility class for assessing the readability of a given text.
    Uses textstat to determine readability scores.
    """

    @staticmethod
    def get_readability(text_str: str) -> float:
        """
        Calculates the readability score of a given text string.

        The function removes URLs, email addresses, hashtags, and mentions before processing the text.

        Args:
            text_str (str): The input text to analyze.

        Returns:
            float: The readability score of the cleaned text.

        Raises:
            ValueError: If the input text is empty or None.
            RuntimeError: If an unexpected error occurs during processing.
        """
        try:
            if not text_str.strip():
                raise ValueError("Input text cannot be empty or only whitespace.")

            # Remove URLs, emails, hashtags, and mentions
            cleaned_text = ' '.join(
                re.sub(r"((\Swww.[^\s]+)|(\S*@\S*\s?)|#[A-Za-z0-9]+)|(@[A-Za-z0-9]+)|(https?:[^\s]+)",
                       " ", text_str, flags=re.MULTILINE).split())

            return textstat.text_standard(cleaned_text, float_output=True)

        except ValueError as ve:
            raise ve  # Re-raise specific validation errors
        except Exception as e:
            raise Runtim
