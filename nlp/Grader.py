import math
from textstat.textstat import textstatistics
from nlp.StanzaNLP import StanzaNLP


class Grader:
    def __init__(self, nlp):
        self.nlp = nlp
        self.sentences = nlp.sentences
        self.sentence_count = len(nlp.sentences)
        self.words = self.get_words()
        self.word_count = len(self.words)
        self.avg_sentence_length = float(self.word_count / self.sentence_count)

    def get_words(self):
        try:
            words = []
            for sentence in self.sentences:
                words += [word.text for word in sentence.words if word.upos not in ['PUNCT', 'PART', 'SYM']]
            return words
        except Exception:
            raise

    @staticmethod
    def syllables_count(word):
        return textstatistics().syllable_count(word)

    def avg_syllables_per_word(self):
        syllable_count = 0
        for word in self.words:
            syllable_count += self.syllables_count(word)
        ASPW = float(syllable_count) / float(self.word_count)
        return math.floor(ASPW)

    def difficult_words(self):
        # difficult words are those with syllables >= 2
        # easy_word_set is provide by Textstat as
        # a list of common words
        diff_words_set = set()
        no_stopwords = StanzaNLP.remove_stopwords(self.sentences)
        for word in no_stopwords:
            syllable_count = self.syllables_count(word.text)
            if syllable_count >= 2:
                diff_words_set.add(word.text)
        return len(diff_words_set)

    def poly_syllable_count(self):
        count = 0
        for word in self.words:
            syllable_count = self.syllables_count(word)
            if syllable_count >= 3:
                count += 1
        return count

    def flesch_kincaid_readability_test(self):
        """
        Implements Flesch Formula:
        Reading Ease score = 206.835 - (1.015 × ASL) - (84.6 × ASW)
        Here,
        ASL = average sentence length (number of words
                divided by number of sentences)
        ASW = average word length in syllables (number of syllables
                divided by number of words)

        100.0–90.0	5th grade
        90.0–80.0	6th grade
        80.0–70.0	7th grade
        70.0–60.0	8th & 9th grade
        60.0–50.0	10th to 12th grade
        50.0–30.0	College
        30.0–10.0	College graduate
        10.0–0.0	Professional
        """
        FRE = 206.835 - float(1.015 * self.avg_sentence_length) - float(84.6 * self.avg_syllables_per_word())
        return math.floor(FRE)

    def gunning_fog(self):
        """
            17	College graduate
            16	College senior
            15	College junior
            14	College sophomore
            13	College freshman
            12	High school senior
            11	High school junior
            10	High school sophomore
            9	High school freshman
            8	Eighth grade
            7	Seventh grade
            6	Sixth grade
        """
        per_diff_words = (self.difficult_words() / self.word_count * 100) + 5
        grade = 0.4 * (self.avg_sentence_length + per_diff_words)
        return math.floor(grade)

    def smog_index(self):
        """
            Implements SMOG Formula / Grading
            SMOG grading = 3 + ?polysyllable count.
            Here,
            polysyllable count = number of words of more
            than two syllables in a sample of 30 sentences.
        """

        if self.sentence_count >= 3:
            poly_syllab = self.poly_syllable_count()
            SMOG = (1.043 * (30 * (poly_syllab / self.sentence_count)) ** 0.5) + 3.1291
            return math.floor(SMOG)
        else:
            return 0

    def dale_chall_readability_score(self):
        """
            Implements Dale Challe Formula:
            Raw score = 0.1579*(PDW) + 0.0496*(ASL) + 3.6365
            1995 updated version
            Raw score = 64 - 0.95 *(PDW) - 0.69 *(ASL)
            Here,
                PDW = Percentage of difficult words.
                ASL = Average sentence length

            4.9 or lower   4th-grade student or lower
            5.0–5.9	5th- or 6th-grade student
            6.0–6.9	7th- or 8th-grade student
            7.0–7.9	9th- or 10th-grade student
            8.0–8.9	11th- or 12th-grade student
            9.0–9.9	college student
        """

        count = self.word_count - self.difficult_words()
        if self.word_count > 0:
            per = float(count) / float(self.word_count) * 100
        diff_words = 100 - per
        raw_score = (0.1579 * diff_words) + (0.0496 * self.avg_sentence_length)
        if diff_words > 5:
            raw_score += 3.6365
        return math.floor(raw_score)
