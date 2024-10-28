import json
from utils.display import table_serialization


def get_annotate_questions_task_shots_with_low_header_sim():
    return "\n".join([
        "\n".join([
            f"**Example {idx + 1}**",
            "- **Gold table set schema**",
            "\n".join([
                table_serialization(
                    table_num=tdx + 1,
                    metadata=table['metadata'],
                    header=table['header'],
                    cell=None
                )
                for tdx, table in enumerate(shot['gold_table_set'])
            ]),
            "",
            "- **NL query list**",
            "\n".join([
                f"Query {data['idx']}: {data['nl_query']}"
                for _, data in enumerate(shot['data_list'])
            ]),
            "",
            "- **Output**:",
            "\n".join([
                "\n".join([
                    f"Annotated question {ddx + 1}:",
                    f"Question: {data['question']}",
                    # f"Type: {data['type']}",
                    ""
                ])
                for ddx, data in enumerate(reversed(shot['annotation']))
            ])
        ])
        for idx, shot in enumerate(SHOTS_WITH_LOW_HEADER_SIM)
    ])


def get_annotate_questions_task_shots_with_high_header_sim():
    return "\n".join([
        "\n".join([
            f"**Example {idx + 1}**",
            "- **Gold table set titles**",
            "\n".join(
                table_serialization(
                    table_num=tdx + 1,
                    metadata=table['metadata'],
                    header=None,
                    cell=None
                ) # Table 1. TEXT
                for tdx, table in enumerate(shot['gold_table_set'])
            ), # Table 1 ~ ith
            "",
            "- **NL query list**",
            "\n".join([
                f"Query {data['idx']}: {data['nl_query']}"
                for _, data in enumerate(shot['data_list'])
            ]), # NL query 1 ~ ith
            "",
            "- **Output**:",
            "\n".join([
                "\n".join([
                    f"Annotated question {ddx + 1}:",
                    f"Question: {data['question']}",
                    # f"Type: {data['type']}",
                    ""
                ]) # Question 1:\n Question \n Type
                for ddx, data in enumerate(reversed(shot['annotation']))
            ]) # Question 1 ~ ith
        ])
        for idx, shot in enumerate(SHOTS_WITH_HIGH_HEADER_SIM)
    ])


def get_expand_statement_task_shots_with_low_header_sim():
    return "\n".join([
        "\n".join([
            f"**Example {idx * 2 + ddx + 1}:**",
            "- **DataFrame schema set**:",
            "\n".join([
                "\n".join([
                    f"\t- **DataFrame {tdx + 1}**:",
                    f"\t- **Caption**: {table['metadata']}",
                    f"\t- **Columns**: {' | '.join(table['header'])}",
                    f"\t- **First row**: {' | '.join(table['cell'][0])}",
                    ""
                ])
                for tdx, table in enumerate(shot['gold_table_set'])
            ]),
            "- **SQL query**:",
            shot['data_list'][ddx]['sql_query'],
            "",
            "- **Statement**:",
            shot['data_list'][ddx]['short_statement'],
            "",
            "- **Python code**:",
            shot['data_list'][ddx]['python_code'],
            ""
        ])
        for idx, shot in enumerate(SHOTS_WITH_LOW_HEADER_SIM)
        for ddx in range(2)
    ])


def get_expand_statement_task_shots_with_high_header_sim():
    return "\n".join([
        "\n".join([
            f"**Example {idx + 1}:**",
            "- **DataFrame schema**:",
            f"\t- **Caption**: {shot['gold_table_set'][0]['metadata']}",
            f"\t- **Columns**: {' | '.join(shot['gold_table_set'][0]['header'])}",
            f"\t- **First row**: {' | '.join(shot['gold_table_set'][0]['cell'][0])}",
            "",
            "- **SQL query**:",
            shot['data_list'][0]['sql_query'],
            "",
            "- **Statement**:",
            shot['data_list'][0]['short_statement'],
            "",
            "- **Python code**:",
            shot['data_list'][0]['python_code'],
            ""
        ])
        for idx, shot in enumerate(SHOTS_WITH_HIGH_HEADER_SIM)
    ])


if __name__ == 'get_shots':
    with open('shots/shots_with_high_header_sim.json', 'r') as file:
        SHOTS_WITH_HIGH_HEADER_SIM = json.load(file)
    MOD_FOUR = 0
    
    with open('shots/shots_with_low_header_sim.json', 'r') as file:
        SHOTS_WITH_LOW_HEADER_SIM = json.load(file)
