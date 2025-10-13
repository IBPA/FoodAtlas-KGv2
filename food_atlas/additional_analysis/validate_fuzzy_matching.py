from itertools import combinations

import networkx as nx
import pandas as pd
from dotenv import load_dotenv
from google import genai
from google.genai import types
from openai import OpenAI
from thefuzz import fuzz
from tqdm import tqdm

from food_atlas.kg.merge_dbs.flavordb.food_flavors import generate_flavor_metadata

load_dotenv()
openai_client = OpenAI()
gemini_client = genai.Client()


system_prompt = """
You are a helpful annotator to help me label whether a flavor label is correct given a \
flavor description.

You will be given a pair of a freeform flavor description and a flavor label.

Your job is to annotate whether the flavor label is correct given the flavor \
description. Your answer must be either "true" or "false".

### Examples:

Example 1:

Flavor description: "Sweetish odor"
Flavor label: "sweet"
Answer: "true"
Explanation: "Sweetish odor" correctly describes the flavor "sweet".

Example 2:

Flavor description: "Pungent, garlic-like odor"
Flavor label: "garlic"
Answer: "true"
Explanation: "Pungent, garlic-like odor" correctly describes the flavor "garlic".

Example 3:

Flavor description: "Characteristic odor resembling that of chloroform"
Flavor label: "characteristic odor"
Answer: "false"
Explanation: "While characteristic odor appears in the description, it misses the \
important detail that it resembles that of chloroform".

### Notes:

The explanation in the examples are given for guidance only. You output should be a \
single word answer, "true" or "false".
"""

user_prompt = """
Flavor descriptor: {flavor_descriptor}
Flavor label: {flavor_label}
Answer: """


def sample_for_flavor_fuzz_validation():
    entities_without_flavor_path = "outputs/kg/entities.tsv"
    entities = pd.read_csv(entities_without_flavor_path, delimiter="\t")
    entities = entities.query("entity_type != 'flavor'")

    # Paths to data used to generate flavor data
    flavordb_data_path = "data/FlavorDB/flavordb_scrape.pt"

    generate_flavor_metadata(flavordb_data_path, entities)


def analyze_flavor_fuzz():
    flavor_fuzz_matches_path = "outputs/additional_analysis/flavor_fuzz_matches.tsv"
    flavor_fuzz_matches = pd.read_csv(flavor_fuzz_matches_path, delimiter="\t")
    print(f"Total number of flavor fuzz matches: {len(flavor_fuzz_matches)}")
    print(
        f"{len(flavor_fuzz_matches['original'].unique())} original flavor descriptors"
    )

    flavor_fuzz_matches = flavor_fuzz_matches.drop_duplicates(
        subset=["original", "matched"]
    )
    print(f"Number of unique flavor fuzz matches: {len(flavor_fuzz_matches)}")
    print(flavor_fuzz_matches)

    flavor_fuzz_matches.to_csv("unique_flavor_fuzz_matches.tsv", sep="\t", index=False)

    return flavor_fuzz_matches


def llm_as_a_judge_openai(flavor_fuzz_matches):
    flavor_fuzz_matches["answer_openai"] = None
    for index, row in tqdm(
        flavor_fuzz_matches.iterrows(), total=len(flavor_fuzz_matches)
    ):
        flavor_descriptor = row["original"]
        flavor_label = row["matched"]
        response = (
            openai_client.chat.completions.create(
                model="gpt-4.1-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {
                        "role": "user",
                        "content": user_prompt.format(
                            flavor_descriptor=flavor_descriptor,
                            flavor_label=flavor_label,
                        ),
                    },
                ],
                n=1,
                temperature=0.0,
            )
            .choices[0]
            .message.content
        )

        flavor_fuzz_matches.at[index, "answer_openai"] = response

    flavor_fuzz_matches.to_csv(
        "unique_flavor_fuzz_matches_openai.tsv", sep="\t", index=False
    )


def llm_as_a_judge_gemini(flavor_fuzz_matches):
    flavor_fuzz_matches["answer_gemini"] = None
    for index, row in tqdm(
        flavor_fuzz_matches.iterrows(), total=len(flavor_fuzz_matches)
    ):
        flavor_descriptor = row["original"]
        flavor_label = row["matched"]
        response = gemini_client.models.generate_content(
            model="gemini-2.5-flash",
            config=types.GenerateContentConfig(
                system_instruction=system_prompt,
            ),
            contents=user_prompt.format(
                flavor_descriptor=flavor_descriptor,
                flavor_label=flavor_label,
            ),
        )
        flavor_fuzz_matches.at[index, "answer_gemini"] = response.text

        print(flavor_descriptor, flavor_label, response.text)

    flavor_fuzz_matches.to_csv(
        "unique_flavor_fuzz_matches_gemini.tsv", sep="\t", index=False
    )


def compare_openai_and_gemini():
    openai_matches = pd.read_csv(
        "unique_flavor_fuzz_matches_openai.tsv", delimiter="\t"
    )
    gemini_matches = pd.read_csv(
        "unique_flavor_fuzz_matches_gemini.tsv", delimiter="\t"
    )

    data = pd.concat([openai_matches, gemini_matches], axis=1)
    data.columns = [
        "drop",
        "original",
        "matched",
        "score",
        "answer_openai",
        "drop",
        "drop",
        "drop",
        "drop",
        "answer_gemini",
    ]
    data = data[["original", "matched", "score", "answer_openai", "answer_gemini"]]

    data["alignment"] = data["answer_openai"] == data["answer_gemini"]

    data.to_csv("unique_flavor_fuzz_matches_comparison.tsv", sep="\t", index=False)


def print_statistics():
    data = pd.read_csv(
        "unique_flavor_fuzz_matches_comparison_human.tsv", delimiter="\t"
    )

    print(f"Alignment rate: {data['alignment'].mean()}")

    # Calculate error rate.
    n_error = (not data[data["alignment"]]["answer_openai"]).sum()
    n_error += (not data[not data["alignment"]]["human"]).sum()
    print(f"Error rate: {n_error / len(data)}")


def estimate_duplicates():
    # Estimate duplicate rate.
    data = pd.read_csv(
        "unique_flavor_fuzz_matches_comparison_human.tsv", delimiter="\t"
    )
    unique_flavor_labels = set(data["matched"])
    print(len(unique_flavor_labels))

    label_pairs = []
    for label1, label2 in combinations(unique_flavor_labels, 2):
        label_pairs.append((label1, label2))

    for thres in [80, 85, 90, 95]:
        G = nx.Graph()
        G.add_nodes_from(unique_flavor_labels)
        for label_pair in label_pairs:
            if fuzz.ratio(label_pair[0], label_pair[1]) > thres:
                G.add_edge(label_pair[0], label_pair[1])

        n_dup = 0
        clusters = list(nx.connected_components(G))
        for cluster in clusters:
            n_dup += len(cluster) - 1
        print(f"Threshold: {thres}")
        print(f"Number of clusters: {len(clusters)}")
        print(f"Number of duplicates: {n_dup}")
        print(f"Duplicate rate: {n_dup / len(unique_flavor_labels)}")
        print()


if __name__ == "__main__":
    # sample_for_flavor_fuzz_validation()
    # flavor_fuzz_matches = analyze_flavor_fuzz()
    # llm_as_a_judge_openai(flavor_fuzz_matches)
    # llm_as_a_judge_gemini(flavor_fuzz_matches)
    # compare_openai_and_gemini()
    print_statistics()
    estimate_duplicates()
