import json
from utils.display import table_serialization


def get_expand_statement_with_high_header_sim_task_shots():
    return "\n".join([
        "\n".join([
            f"**Example {idx + 1}:**",
            "- **DataFrame Schema**:",
            f"DataFrame [caption] {shot['gold_table_set'][0]['metadata']} " + \
            f"[columns] {' | '.join(shot['gold_table_set'][0]['header'])} " + \
            f"[first row] {' | '.join(shot['gold_table_set'][0]['cell'][0])}",
            "",
            "- **SQL Query**:",
            shot['data_list'][0]['sql_query'],
            "",
            "- **Statement**:",
            shot['data_list'][0]['short_statement'],
            "",
            "- **Python Code**:",
            shot['data_list'][0]['python_code'],
            ""
        ])
        for idx, shot in enumerate(SHOTS_WITH_HIGH_HEADER_SIM)
    ])


def get_annotate_questions_with_high_header_sim_task_shots():
    return "\n".join([
        "\n".join([
            f"**Example {idx + 1}:**",
            "- **Table Title Set**:",
            "\n".join([
                table_serialization(
                    table_num=tdx + 1,
                    metadata=table['metadata'],
                    header=None,
                    cell=None
                )
                for tdx, table in enumerate(shot['gold_table_set'])
            ]),
            "",
            "- **Output**:",
            "\n".join([
                f"Question {qdx + 1}: {annotation['question']}"
                for qdx, annotation in enumerate(shot['annotation'])
            ]),
            ""
        ])
        for idx, shot in enumerate(SHOTS_WITH_HIGH_HEADER_SIM)
    ])


###


def get_expand_statement_with_low_header_sim_task_shots():
    return "\n".join([
        "\n".join([
            f"**Example {idx * 2 + ddx + 1}:**",
            "- **DataFrame Schema**:",
            f"DataFrame [caption] {shot['joined_table']['metadata']} " + \
            f"[columns] {' | '.join(shot['joined_table']['header'])} " + \
            f"[first row] {' | '.join(shot['joined_table']['cell'][0])}",
            "",
            "- **SQL Query**:",
            shot['data_list'][ddx]['sql_query'],
            "",
            "- **Statement**:",
            shot['data_list'][ddx]['short_statement'],
            "",
            "- **Python Code**:",
            shot['data_list'][ddx]['python_code'],
            ""
        ])
        for idx, shot in enumerate(SHOTS_WITH_LOW_HEADER_SIM)
        for ddx in range(2)
    ])


def get_annotate_questions_with_low_header_sim_task_shots():
    return "\n".join([
        "\n".join([
            f"**Example {idx + 1}:**",
            "- **Table Title Set**:",
            "\n".join([
                table_serialization(
                    table_num=tdx + 1,
                    metadata=table['metadata'],
                    header=None,
                    cell=None
                )
                for tdx, table in enumerate(shot['gold_table_set'])
            ]),
            "",
            "- **Output**:",
            "\n".join([
                f"Question {qdx + 1}: {annotation['question']}"
                for qdx, annotation in enumerate(shot['annotation'])
            ]),
            ""
        ])
        for idx, shot in enumerate(SHOTS_WITH_LOW_HEADER_SIM)
    ])


if __name__ == 'get_shots':
    with open('shots/shots_with_high_header_sim.json', 'r') as file:
        SHOTS_WITH_HIGH_HEADER_SIM = json.load(file)
    
    with open('shots/shots_with_low_header_sim.json', 'r') as file:
        SHOTS_WITH_LOW_HEADER_SIM = json.load(file)
