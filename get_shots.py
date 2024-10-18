import json
from itertools import product
from utils.display import table_serialization


def get_annotate_questions_task_shots():
    global MOD_FOUR
    sampled_shots = [SHOTS[MOD_FOUR], SHOTS[4], SHOTS[5]]
    MOD_FOUR = (MOD_FOUR + 1) % 4

    return "\n".join(
        "\n".join([
            f"## Example {idx + 1}",
            "# Gold table set information",
            "\n".join(
                table_serialization(
                    table_num=tdx + 1,
                    metadata=table['metadata'],
                    header=table['header'],
                    cell=table['cell']
                ) # Table 1. TEXT
                for tdx, table in enumerate(shot['gold_table_set'])
            ), # Table 1 ~ ith
            "",
            "# NL query list",
            "\n".join([
                (
                    f"Query {data['idx']} [Entail to table "
                    + ", ".join([
                        str(tdx + 1)
                        for tdx, gold_table_id in enumerate([table['id'] for table in shot['gold_table_set']])
                        if gold_table_id in data['entailed_table_id_set']
                    ])
                    + f"] {data['nl_query']}"

                ) # NL query 1 [Table 1] TEXT
                for _, data in enumerate(shot['data_list'])
            ]), # NL query 1 ~ ith
            "",
            "# Output",
            "\n".join([
                "\n".join([
                    f"Annotated question {ddx + 1}:",
                    f"Question: {data['question']}",
                    f"Difficulty: {data['difficulty']}",
                    f"Reference: {str(data['reference'])}",
                    ""
                ]) # Question 1:\n Question \n Difficulty \n Reference \n
                for ddx, data in enumerate(reversed(shot['annotation']))
            ]) # Question 1 ~ ith
        ])
        for idx, shot in enumerate(sampled_shots)
    )


def get_annotate_sub_answer_task_shots():
    sampled_shots = SHOTS

    return "\n".join([
        "\n".join([
            f"## Example {idx + 1}",
            "# Table information",
            table_serialization(
                table_num=-1,
                metadata=shot['gold_table_set'][0]['metadata'],
                header=shot['gold_table_set'][0]['header'],
                cell=shot['gold_table_set'][0]['cell']
            ),
            "",
            "# Question",
            shot['annotation'][-1]['question'],
            "",
            "# Output",
            shot['annotation'][-1]['sub_answer'],
            ""
        ])
        for idx, shot in enumerate(sampled_shots)
    ])


if __name__ == 'get_shots':
    with open('shots/shots.json', 'r') as file:
        SHOTS = json.load(file)
    
    MOD_FOUR = 3
