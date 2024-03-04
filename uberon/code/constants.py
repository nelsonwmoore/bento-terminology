"""constants for loading term sets"""

class Attr:
    """bento-meta attribute constants"""
    HANDLE = "handle"
    KEY = "key"
    VALUE = "value"
    MAP_SRC = "mapping_source"
    O_NAME = "origin_name"
    O_ID = "origin_id"
    O_DEFINITION = "origin_definition"
    O_PREF_TERM = "origin_preferred_term"
    VALUE_DOMAIN = "value_domain"

class NCIt:
    """loading term sets NCIt constants"""
    HANDLE = "NCIt"
    PREF_TERM = "NCIt Preferred Term"
    CODE = "NCIt Concept Code"
    DEFINITION = "NCIt Definition"

class Uberon:
    """loading uberon term set constants"""
    INPUT_FILE = "source-data/UBERON_Terminology.xls"
    HANDLE = "UBERON"
    SYNONYM_SEP = " || "
    OUTPUT_FILE = "model-desc/uberon_terms.yml"
    PREF_TERM = "UBERON Preferred Term"
    CODE = "UBERON Code"
    DEFINITION = "UBERON Definition"
    SYNONYMS = "UBERON Synonyms(s)"
