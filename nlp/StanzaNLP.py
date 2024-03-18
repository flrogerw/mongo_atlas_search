import stanza
from stanza.models.common.doc import Document
from stanza.pipeline.multilingual import MultilingualPipeline

lang_id_config = {"langid_clean_text": True}
lang_configs = {"en": {"processors": {"ner": "conll03"}}}
nlp = MultilingualPipeline(lang_id_config=lang_id_config, lang_configs=lang_configs)
docs = ["Na Shi Hou Wo Men You Meng Guan Yu Wen Xue Guan Yu Ai Qing Guan Yu Chuan Yue Shi Jie De Lu Xing . Ru Jin Wo Men Shen Ye Yin Jiu Jiu Zhao Yue Se Rang Ni Lai Ting Ting Wo De Gu Shi."," Powered by Firstory Hosting"]
docs = nlp(docs)
for doc in docs:
    print("---")
    print(f"text: {doc.text}")
    print(f"lang: {doc.lang}")
    print(f"{doc.sentences[0].dependencies_string()}")
