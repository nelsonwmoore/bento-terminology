"""load uberon terms and convert to MDF (source: https://evs.nci.nih.gov/ftp1/UBERON/About.html)"""
import os
import re
from typing import Any

import polars as pl
import yaml
from bento_mdf.mdf import MDF
from bento_meta.entity import Entity
from bento_meta.model import Model
from bento_meta.objects import Concept, Node, Property, Tag, Term
from constants import Attr, NCIt, Uberon


def to_snake_case(string: str) -> str:
    """converts given string to snake case representation"""
    string = string.replace(" ", "_")
    string = re.sub(r"(?<=[a-z0-9])_?([A-Z])", r"_\1", string)
    return string.lower()

def add_tag_to_entity(entity: Entity, key: str, val: str) -> None:
    """add a bento-meta Tag object with given key & val to an Entity's tags collection"""
    tag = Tag({Attr.KEY: key, Attr.VALUE: val})
    entity.tags[tag.key] = tag


def add_uberon_row_syns_to_concept(row: dict[str, Any], concept: Concept) -> None:
    """add uberon synonyms from row as terms linked to concept"""
    if not row.get(Uberon.SYNONYMS):
        return
    for synonym_val in row[Uberon.SYNONYMS].split(Uberon.SYNONYM_SEP):
        synonym_term = Term({
            Attr.HANDLE: to_snake_case(synonym_val),
            Attr.VALUE : synonym_val,
            Attr.O_NAME: Uberon.HANDLE,
            Attr.O_ID: row[Uberon.CODE],
            Attr.O_DEFINITION: row[Uberon.DEFINITION]
        })
        concept.terms[synonym_term.value] = synonym_term

def add_ncit_row_syn_to_concept(row: dict[str, Any], concept: Concept) -> None:
    """add ncit synonym from row as term linked to concept"""
    ncit_term = Term({
        Attr.HANDLE: to_snake_case(row[NCIt.PREF_TERM]),
        Attr.VALUE : row[Uberon.PREF_TERM],
        Attr.O_NAME: NCIt.HANDLE,
        Attr.O_ID: row[NCIt.CODE],
        Attr.O_DEFINITION: row[NCIt.DEFINITION]
    })
    concept.terms[ncit_term.value] = ncit_term
    # add tag indicating the mapping source (NCIt)
    add_tag_to_entity(entity=concept, key=Attr.MAP_SRC, val=NCIt.HANDLE)

def row_to_term(row: dict[str, Any]) -> Term:
    """get preferred uberon term from row of polars df"""
    pref_term = Term({
        Attr.HANDLE: to_snake_case(row[Uberon.PREF_TERM]),
        Attr.VALUE: row[Uberon.PREF_TERM],
        Attr.O_NAME: Uberon.HANDLE,
        Attr.O_ID: row[Uberon.CODE],
        Attr.O_DEFINITION: row[Uberon.DEFINITION]
    })
    # add tag indicating this is uberon preferred term
    add_tag_to_entity(entity=pref_term, key=Attr.O_PREF_TERM, val=Attr.O_PREF_TERM)
    # add synonyms (NCIt & any UBERON Synonyms) to concept of pref_term
    concept = Concept()
    add_ncit_row_syn_to_concept(row=row, concept=concept)
    add_uberon_row_syns_to_concept(row=row, concept=concept)
    pref_term.concept = concept

    return pref_term

def get_terms_from_df(df: pl.DataFrame) -> list[Term]:
    """get terms from uberon term df"""
    return [row_to_term(row=row) for row in df.iter_rows(named=True)]

def filter_and_save_terms_mdf(file: str) -> None:
    """loads MDF and saves again with only the 'Terms' section"""
    with open(file=file, mode="r", encoding="utf-8") as full_mdf:
        full_data=yaml.safe_load(full_mdf)

    terms_data = {"Terms": full_data.get("Terms", {})}

    with open(file=file, mode="w", encoding="utf-8") as terms_mdf:
        yaml.dump(data=terms_data, stream=terms_mdf, indent=4)

def main():
    """ETL for Uberon Terminology to MDF"""

    # get script's directory for relative paths to input/output files
    script_dir = os.path.dirname(os.path.abspath(__file__))
    uberon_dir = os.path.join(script_dir, "..")
    input_file = os.path.join(uberon_dir, Uberon.INPUT_FILE)
    output_file = os.path.join(uberon_dir, Uberon.OUTPUT_FILE)

    # load Uberon sheet into polars df
    uberon_df = pl.read_excel(input_file)

    # get list of terms from df
    terms = get_terms_from_df(df=uberon_df)

    # initialize bento-meta Model
    model = Model(handle=Uberon.HANDLE)

    # add placeholder node/prop to model
    node = Node({Attr.HANDLE: "TEMP_NODE"})
    prop = Property({
        Attr.HANDLE: "TEMP_PROP", Attr.VALUE_DOMAIN: "value_set"
    })
    model.add_node(node=node)
    model.add_prop(ent=node, prop=prop)

    # add terms to model
    model.add_terms(prop, *terms)

    # save model to MDF
    mdf = MDF(handle=Uberon.HANDLE, model=model)
    mdf.write_mdf(file=output_file)

    # remove placeholder node/prop from MDF
    filter_and_save_terms_mdf(file=output_file)


if __name__ == "__main__":
    main()
